import os
import mysql.connector
import requests
import smtplib
import pytz
import traceback

from datetime import datetime, date, time as dt_time, timedelta, timezone
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587
EMAIL_USER = "testwebservice71@gmail.com"
EMAIL_PASS = "akuu vulg ejlg ysbt"
# ================== CONFIG ==================
db_config = {
    "host": "switchback.proxy.rlwy.net",
    "user": "root",
    "port": 44750,
    "password": "qYxlhEiaEvtiRvKaFyigDPtXSSCpddMv",
    "database": "railway",
    "raise_on_warnings": True,
}

SMS_API_URL = "https://www.universalsmsadvertising.com/universalsmsapi.php"
SMS_USER = "8960853914"
SMS_PASS = "8960853914"
SENDER_ID = "FRTLLP"

OFFLINE_THRESHOLD = 5         # minutes
SECOND_NOTIFICATION_HOURS = 6  # hours

# Brevo (Sendinblue) API key from env
BREVO_API_KEY = os.getenv("BREVO_API_KEY")  # set this in Railway/Env
EMAIL_USER = "fertisenseiot@gmail.com"
# timezone objects (kept for compatibility)
IST_PYTZ = pytz.timezone("Asia/Kolkata")
IST = timezone(timedelta(hours=5, minutes=30))

# ================== HELPERS ==================
def log(msg):
    """Simple timestamped logger."""
    print(f"[{datetime.now().isoformat(sep=' ', timespec='seconds')}] {msg}")


def build_message(ntf_typ, devnm):
    """Return short SMS/plain message text for given type and device name."""
    messages = {
        3: f"WARNING!! The {devnm} is offline. Please take necessary action - Regards Fertisense LLP",
        5: f"INFO!! The device {devnm} is back online. No action is required - Regards Fertisense LLP",
    }
    return messages.get(ntf_typ, f"Alert for {devnm} - Regards Fertisense LLP")
# ---------------- send sms ----------------
def send_sms(phone, message):
    """
    Send SMS via configured provider.
    Return True if HTTP status is 200 (best-effort).
    """
    if not phone:
        return False
    try:
        params = {
            "user_name": SMS_USER,
            "user_password": SMS_PASS,
            "mobile": phone,
            "sender_id": SENDER_ID,
            "type": "F",
            "text": message
        }
        r = requests.get(SMS_API_URL, params=params, timeout=30)
        text_preview = r.text.replace("\n", " ")[:400]
        log(f"SMS API -> phone={phone} status_code={r.status_code} text={text_preview}")
        return r.status_code == 200
    except Exception as e:
        log(f"❌ SMS failed for {phone}: {e}")
        return False

# ---------------- Email templates (HTML) ----------------
def offline_html(dev_name, diff_minutes=None):
    uptime_text = ""
    if diff_minutes is not None:
        uptime_text = f"<p><strong>Last seen:</strong> {diff_minutes:.1f} minutes ago</p>"
    return f"""
    <html>
      <body>
        <h2 style="color:#b02a2a">⚠ {dev_name} is <em>OFFLINE</em></h2>
        {uptime_text}
        <p>Please take necessary action to restore connectivity.</p>
        <hr>
        <small>Regards,<br/>Fertisense LLP</small>
      </body>
    </html>
    """


def online_html(dev_name):
    return f"""
    <html>
      <body>
        <h2 style="color:#2a9d2a">✔ {dev_name} is <em>BACK ONLINE</em></h2>
        <p>No action required.</p>
        <hr>
        <small>Regards,<br/>Fertisense LLP</small>
      </body>
    </html>
    """


# ================== EMAIL SENDING ==================
def send_email_brevo(subject, html_content, email_ids):
    """
    Send email via Brevo (Sendinblue) transactional API.
    Returns True on success, False otherwise.
    """
    if not email_ids:
        return False

    api_key = BREVO_API_KEY
    if not api_key:
        log("ℹ️ BREVO_API_KEY not set — skipping Brevo and will fallback to SMTP.")
        return False

    url = "https://api.brevo.com/v3/smtp/email"
    headers = {
        "accept": "application/json",
        "content-type": "application/json",
        "api-key": api_key
    }

    payload = {
        "sender": {"name": "Fertisense LLP", "email": EMAIL_USER},
        "to": [{"email": e} for e in email_ids],
        "subject": subject,
        "htmlContent": html_content
    }

    try:
        r = requests.post(url, json=payload, headers=headers, timeout=20)
        text_preview = (r.text or "")[:800].replace("\n", " ")
        log(f"Brevo API -> status={r.status_code} resp={text_preview}")
        # Brevo returns 201 on success for send; accept 200/201 as success
        return r.status_code in (200, 201)
    except Exception as e:
        log(f"❌ Brevo send failed: {e}")
        return False


def send_email_smtp(subject, html_content, email_ids):
    """
    Fallback SMTP HTML email sender.
    Returns True on success, False otherwise.
    """
    if not email_ids:
        return False
    try:
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"] = EMAIL_USER
        msg["To"] = ", ".join(email_ids)
        part_html = MIMEText(html_content, "html")
        msg.attach(part_html)

        server = smtplib.SMTP(SMTP_SERVER, SMTP_PORT, timeout=20)
        server.starttls()
        server.login(EMAIL_USER, EMAIL_PASS)
        server.sendmail(EMAIL_USER, email_ids, msg.as_string())
        server.quit()
        log(f"✅ SMTP Email sent to {len(email_ids)} recipients")
        return True
    except Exception as e:
        log(f"❌ SMTP Email failed: {e}")
        return False


def send_email(subject, html_content, email_ids):
    """
    Top-level email function:
    1) try Brevo (if key present)
    2) fallback to SMTP
    Returns True/False.
    """
    # 1) Try Brevo first (if key)
    sent = False
    if BREVO_API_KEY:
        try:
            sent = send_email_brevo(subject, html_content, email_ids)
            if sent:
                return True
            else:
                log("ℹ️ Brevo attempt returned False — trying SMTP fallback.")
        except Exception as e:
            log(f"❌ Error when calling Brevo: {e}")
            sent = False

    # 2) Fallback to SMTP
    try:
        sent = send_email_smtp(subject, html_content, email_ids)
    except Exception as e:
        log(f"❌ SMTP fallback failed: {e}")
        sent = False

    return bool(sent)


# ================== CONTACTS FETCH ==================
def get_contact_info(device_id):
    """
    Return (phones_list, emails_list, org_id, centre_id)
    If subscription invalid -> returns ([], [], org_id, centre_id) or ([], [], 1, 1)
    """
    conn = None
    cursor = None
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)

        today = date.today()

        # Subscription check (Subscription_ID = 8)
        cursor.execute("""
            SELECT sh.*, msi.Package_Name
            FROM Subcription_History sh
            JOIN Master_Subscription_Info msi
              ON sh.Subscription_ID = msi.Subscription_ID
            WHERE sh.Device_ID = %s
              AND sh.Subscription_ID = 8
              AND sh.Subcription_End_date >= %s
            ORDER BY sh.Subcription_End_date DESC
            LIMIT 1
        """, (device_id, today))
        subscription = cursor.fetchone()
        log(f"DEBUG subscription for device {device_id}: {subscription}")

        # If no valid subscription, return empty contacts and org/centre as 1 fallback
        if not subscription:
            # still try to fetch org/centre for debug/reporting
            cursor.execute("SELECT ORGANIZATION_ID, CENTRE_ID FROM iot_api_masterdevice WHERE DEVICE_ID=%s", (device_id,))
            device = cursor.fetchone()
            if device:
                return [], [], device.get("ORGANIZATION_ID") or 1, device.get("CENTRE_ID") or 1
            return [], [], 1, 1

        # fetch device org/centre
        cursor.execute("SELECT ORGANIZATION_ID, CENTRE_ID FROM iot_api_masterdevice WHERE DEVICE_ID=%s", (device_id,))
        device = cursor.fetchone()
        if not device:
            return [], [], 1, 1
        org_id = device.get("ORGANIZATION_ID") or 1
        centre_id = device.get("CENTRE_ID") or 1

        # fetch users linked to org+centre
        cursor.execute("""
            SELECT USER_ID_id FROM userorganizationcentrelink
            WHERE ORGANIZATION_ID_id=%s AND CENTRE_ID_id=%s
        """, (org_id, centre_id))
        user_rows = cursor.fetchall()
        user_ids = [r["USER_ID_id"] for r in user_rows] if user_rows else []
        log(f"DEBUG user_ids for org={org_id}, centre={centre_id}: {user_ids}")

        if not user_ids:
            return [], [], org_id, centre_id

        # fetch phone/email + preference
        format_strings = ','.join(['%s'] * len(user_ids))
        cursor.execute(f"""
            SELECT USER_ID, PHONE, EMAIL, SEND_SMS, SEND_EMAIL
            FROM master_user
            WHERE USER_ID IN ({format_strings})
        """, tuple(user_ids))
        users = cursor.fetchall()
        log(f"DEBUG users fetched: {users}")

        phones = []
        emails = []
        for u in users:
            phone = u.get("PHONE") or u.get("phone") or None
            email = u.get("EMAIL") or u.get("email") or None
            send_sms_flag = u.get("SEND_SMS") or u.get("send_sms") or 0
            send_email_flag = u.get("SEND_EMAIL") or u.get("send_email") or 0
            if send_sms_flag == 1 and phone:
                phones.append(str(phone).strip())
            if send_email_flag == 1 and email:
                emails.append(email.strip())

        # dedupe
        phones = list(dict.fromkeys(phones))
        emails = list(dict.fromkeys(emails))

        return phones, emails, org_id, centre_id

    except Exception as e:
        log(f"❌ Error getting contacts for device {device_id}: {e}")
        traceback.print_exc()
        return [], [], 1, 1
    finally:
        if cursor:
            cursor.close()
        if conn and conn.is_connected():
            conn.close()


# ================== PARSERS ==================
def parse_reading_time(val):
    """Normalize READING_TIME from DB to datetime.time"""
    if val is None:
        return None
    # timedelta (sometimes stored)
    if isinstance(val, timedelta):
        total_sec = int(val.total_seconds())
        return dt_time(total_sec // 3600, (total_sec % 3600) // 60, total_sec % 60)
    # already time object
    try:
        if hasattr(val, 'hour'):
            return val  # probably datetime.time
    except Exception:
        pass
    # string "HH:MM:SS" or "H:M:S"
    if isinstance(val, str):
        try:
            parts = [int(x) for x in val.split(':')]
            if len(parts) == 3:
                return dt_time(parts[0], parts[1], parts[2])
            if len(parts) == 2:
                return dt_time(parts[0], parts[1], 0)
        except Exception:
            return None
    return None


def parse_db_time_like(val):
    """Parse values coming from DB SMS_TIME/EMAIL_TIME which can be timedelta/time/string.
       Return datetime.time or None.
    """
    if val is None:
        return None
    if isinstance(val, dt_time):
        return val
    if isinstance(val, timedelta):
        total = int(val.total_seconds())
        return dt_time(total // 3600, (total % 3600) // 60, total % 60)
    if isinstance(val, str):
        # try HH:MM:SS or HH:MM
        try:
            p = [int(x) for x in val.split(':')]
            if len(p) == 3:
                return dt_time(p[0], p[1], p[2])
            if len(p) == 2:
                return dt_time(p[0], p[1], 0)
        except Exception:
            return None
    return None


# ================== MAIN LOGIC ==================
def check_device_online_status():
    conn = None
    cursor = None
    try:
        log("🚀 Starting device online/offline check")
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)

        # Use local 'now' (naive local time) consistently throughout this run
        #now = datetime.now)
        # Optionally, if you prefer explicit IST-aware times, you can use:
        now = datetime.now(IST_PYTZ)

        cursor.execute("SELECT DEVICE_ID, DEVICE_NAME FROM iot_api_masterdevice WHERE DEVICE_STATUS = 1")
        devices = cursor.fetchall()
        log(f"✅ Found {len(devices)} active devices")

        for d in devices:
            # refresh 'now' for each device so timestamps are accurate
            now = datetime.now(IST_PYTZ)
            devid = d["DEVICE_ID"]
            devnm = d.get("DEVICE_NAME") or f"Device-{devid}"
            log(f"\n--- Processing device {devid} : {devnm} ---")

            # subscription + contacts
            phones, emails, org_id, centre_id = get_contact_info(devid)
            log(f"DEBUG contacts -> phones={phones} emails={emails} org={org_id} centre={centre_id}")
            if not phones and not emails:
                log(f"⏹ {devnm} skipped (no valid subscription or no contacts)")
                continue

            # last reading (robust parse)
            cursor.execute("""
                SELECT READING_DATE, READING_TIME
                FROM device_reading_log
                WHERE DEVICE_ID=%s
                ORDER BY READING_DATE DESC, READING_TIME DESC
                LIMIT 1
            """, (devid,))
            last_read = cursor.fetchone()

            diff_minutes = None
            last_update = None

            if last_read:
                rd = last_read.get("READING_DATE")
                rt = parse_reading_time(last_read.get("READING_TIME"))
                if rd and rt:
                    # Combine date+time (naive local)
                    last_update = datetime.combine(rd, rt)
                else:
                    # couldn't parse time properly
                    log(f"DEBUG could not parse READING_TIME: {last_read.get('READING_TIME')}")
            else:
                log("DEBUG no readings found for device; forcing OFFLINE")
            print("Last Updated time",last_update)
            # if last_update: 
            #     diff_minutes = (now - last_update).total_seconds() / 60.0
            #     # fix negative diffs due to clock skew
            #     if diff_minutes < 0:
            #         log(f"⚠ Negative diff_min ({diff_minutes:.1f}) detected — forcing OFFLINE")
            #         diff_minutes = OFFLINE_THRESHOLD + 1.0
            # else:
            #     diff_minutes = OFFLINE_THRESHOLD + 1.0
            if last_update:

    # STEP 1: Convert DB timestamp to IST-aware if it is naive
                if last_update.tzinfo is None:
                    last_update = IST_PYTZ.localize(last_update)

                # STEP 2: now is already IST-aware, so subtraction is safe
                diff_minutes = (now - last_update).total_seconds() / 60.0

                # STEP 3: fix negative diffs due to clock skew
                if diff_minutes < 0:
                    log(f"⚠️ Negative diff_min ({diff_minutes:.1f}) detected — forcing OFFLINE")
                    diff_minutes = OFFLINE_THRESHOLD + 1.0

            else:
                # No last_update available → force offline
                diff_minutes = OFFLINE_THRESHOLD + 1.0


            log(f"DEBUG last_read -> date={last_read.get('READING_DATE') if last_read else None} time={last_read.get('READING_TIME') if last_read else None} last_update={last_update} diff_min={diff_minutes:.1f}")

            current_state = 0 if (diff_minutes is None or diff_minutes > OFFLINE_THRESHOLD) else 1

            # get existing open alarm from device_status_alarm_log
            cursor.execute("""
                SELECT * FROM device_status_alarm_log
                WHERE DEVICE_ID=%s AND IS_ACTIVE=1
                ORDER BY DEVICE_STATUS_ALARM_ID DESC LIMIT 1
            """, (devid,))
            existing_alarm = cursor.fetchone()
            log(f"DEBUG existing_alarm={existing_alarm}")

            # ---------- DEVICE ONLINE ----------
            if current_state == 1:
                log(f"✅ {devnm} is ONLINE (diff_min={diff_minutes:.1f})")
                if existing_alarm:
                    log("➡ Found open offline alarm - will close it and send ONLINE notifications")

                    message = build_message(5, devnm)
                    html = online_html(devnm)

                    sms_sent_any = False
                    email_sent = False

                    if phones:
                        # for ph in phones:
                        #     log(f"DEBUG: attempting ONLINE SMS to {ph} -> message: {message[:120]}")
                        #     ok = send_sms(ph, message)
                        #     log(f"DEBUG: ONLINE SMS send result for {ph} = {ok}")
                        #     if ok:
                        #         sms_sent_any = True
                        # ---- Deduplicate phone numbers (handles comma-separated numbers) ----
                        unique_phones = set()

                        for p in phones:
                            if p:
                                for num in p.split(","):
                                    num = num.strip()
                                    if num:
                                        unique_phones.add(num)

                        log(f"Unique phone numbers for notification: {unique_phones}")

                        # ---- Send SMS to each UNIQUE phone number ----
                        sms_sent_any = False

                        for ph in unique_phones:
                            log(f"DEBUG: attempting ONLINE SMS to {ph} -> message: {message[:120]}")
                            ok = send_sms(ph, message)
                            log(f"DEBUG: ONLINE SMS send result for {ph} = {ok}")
                            if ok:
                                sms_sent_any = True


                    # if emails:
                    #     subj = f"✔ {devnm} is Back Online - Fertisense Update"
                    #     email_sent = send_email(subj, html, emails)
                    #     email_sent = bool(email_sent)
                    #     log(f"DEBUG ONLINE email_sent = {email_sent}")
                    # ---- Deduplicate emails (also handles comma-separated) ----
                    unique_emails = set()

                    for e in emails:
                        if e:
                            for item in e.split(","):
                                item = item.strip()
                                if item:
                                    unique_emails.add(item)

                    log(f"Unique emails for notification: {unique_emails}")

                    # ---- Send email only once ----
                    if unique_emails:
                        subj = f"✓ {devnm} is Back Online - Fertisense Update"
                        email_sent = send_email(subj, html, list(unique_emails))
                        email_sent = bool(email_sent)
                        log(f"DEBUG ONLINE email_sent = {email_sent}")

                    # update alarm: set IS_ACTIVE=0 and update timestamps appropriately
                    try:
                        cursor.execute("""
                            UPDATE device_status_alarm_log
                            SET IS_ACTIVE=0,
                                UPDATED_ON_DATE=%s,
                                UPDATED_ON_TIME=%s,
                                SMS_DATE=%s,
                                SMS_TIME=%s,
                                EMAIL_DATE=%s,
                                EMAIL_TIME=%s
                            WHERE DEVICE_STATUS_ALARM_ID=%s
                        """, (
                            now.date(), now.time(),
                            now.date() if sms_sent_any else existing_alarm.get("SMS_DATE"),
                            now.time() if sms_sent_any else existing_alarm.get("SMS_TIME"),
                            now.date() if email_sent else existing_alarm.get("EMAIL_DATE"),
                            now.time() if email_sent else existing_alarm.get("EMAIL_TIME"),
                            existing_alarm["DEVICE_STATUS_ALARM_ID"]
                        ))
                        conn.commit()
                        log("➡ Alarm closed and DB updated.")
                    except Exception as e:
                        log(f"❌ Failed to update alarm record when closing: {e}")
                        traceback.print_exc()
                else:
                    log("➡ No open alarm; nothing to do.")
                continue

            # ---------- DEVICE OFFLINE ----------
            log(f"🚨 {devnm} is OFFLINE (diff_min={'NA' if diff_minutes is None else f'{diff_minutes:.1f}'})")

            # Case A: create new alarm if none
            if not existing_alarm:
                log("➡ No active alarm exists. Creating new offline alarm and sending initial notifications.")
                message = build_message(3, devnm)
                html = offline_html(devnm, diff_minutes)

                sms_sent_any = False
                email_sent = False

                if phones:
                    # for ph in phones:
                    #     log(f"DEBUG: attempting OFFLINE SMS to {ph} -> message: {message[:120]}")
                    #     ok = send_sms(ph, message)
                    #     log(f"DEBUG: OFFLINE SMS send result for {ph} = {ok}")
                    #     if ok:
                    #         sms_sent_any = True
                    # ---- Deduplicate phone numbers (handles comma-separated numbers) ----
                    unique_phones = set()

                    for p in phones:
                        if p:
                            for num in p.split(","):
                                num = num.strip()
                                if num:
                                    unique_phones.add(num)

                    log(f"Unique phone numbers for notification: {unique_phones}")

                    # ---- Send SMS to each UNIQUE phone number ----
                    sms_sent_any = False

                    for ph in unique_phones:
                        log(f"DEBUG: attempting ONLINE SMS to {ph} -> message: {message[:120]}")
                        ok = send_sms(ph, message)
                        log(f"DEBUG: ONLINE SMS send result for {ph} = {ok}")
                        if ok:
                            sms_sent_any = True

                # if emails:
                #     subj = f"⚠ {devnm} is Offline - Fertisense Alert"
                #     email_sent = send_email(subj, html, emails)
                #     email_sent = bool(email_sent)
                #     log(f"DEBUG initial offline email_sent = {email_sent}")
                # ---- Deduplicate emails (also handles comma-separated) ----
                unique_emails = set()

                for e in emails:
                    if e:
                        for item in e.split(","):
                            item = item.strip()
                            if item:
                                unique_emails.add(item)

                log(f"Unique emails for notification: {unique_emails}")

                # ---- Send email only once ----
                if unique_emails:
                    subj = f"✓ {devnm} is Back Online - Fertisense Update"
                    email_sent = send_email(subj, html, list(unique_emails))
                    email_sent = bool(email_sent)
                    log(f"DEBUG ONLINE email_sent = {email_sent}")

                try:
                    log("DEBUG: about to INSERT new offline alarm into device_status_alarm_log")
                    cursor.execute("""
                        INSERT INTO device_status_alarm_log
                        (DEVICE_ID, DEVICE_STATUS, IS_ACTIVE,
                         CREATED_ON_DATE, CREATED_ON_TIME,
                         SMS_DATE, SMS_TIME, EMAIL_DATE, EMAIL_TIME)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    """, (
                        devid, 1, 1,
                        now.date(), now.time(),
                        now.date() if sms_sent_any else None,
                        now.time() if sms_sent_any else None,
                        now.date() if email_sent else None,
                        now.time() if email_sent else None
                    ))
                    conn.commit()
                    log("➕ New offline alarm created.")
                except Exception as e:
                    log(f"❌ Failed to insert offline alarm: {e}")
                    traceback.print_exc()
                continue

            # Case B: existing offline alarm -> handle SMS + EMAIL timing (6-hour rule)
            log("➡ Active offline alarm exists. Checking notification timing rules.")

            sms_date = existing_alarm.get("SMS_DATE")
            sms_time = existing_alarm.get("SMS_TIME")
            sms_last_dt = None
            try:
                sms_time_parsed = parse_db_time_like(sms_time)
                if sms_date and sms_time_parsed:
                    sms_last_dt = datetime.combine(sms_date, sms_time_parsed)
                    print("sms_last_date",sms_last_dt)
                elif sms_date and not sms_time_parsed:
                    # if only date present, use midnight
                    sms_last_dt = datetime.combine(sms_date, dt_time(0, 0, 0))
            except Exception:
                sms_last_dt = None

            email_date = existing_alarm.get("EMAIL_DATE")
            email_time = existing_alarm.get("EMAIL_TIME")
            email_last_dt = None
            try:
                email_time_parsed = parse_db_time_like(email_time)
                if email_date and email_time_parsed:
                    email_last_dt = datetime.combine(email_date, email_time_parsed)
                elif email_date and not email_time_parsed:
                    email_last_dt = datetime.combine(email_date, dt_time(0, 0, 0))
            except Exception:
                email_last_dt = None

            # If no SMS ever sent, send now (first SMS was missing)
            if not sms_last_dt:
                log("➡ No SMS sent previously for this alarm. Sending now.")
                message = build_message(3, devnm)
                sms_sent_any = False
                if phones:
                    for ph in phones:
                        log(f"DEBUG: attempting offline-first SMS to {ph} -> message: {message[:120]}")
                        if send_sms(ph, message):
                            sms_sent_any = True
                try:
                    cursor.execute("""
                        UPDATE device_status_alarm_log
                        SET SMS_DATE=%s, SMS_TIME=%s
                        WHERE DEVICE_STATUS_ALARM_ID=%s
                    """, (now.date() if sms_sent_any else None, now.time() if sms_sent_any else None, existing_alarm["DEVICE_STATUS_ALARM_ID"]))
                    conn.commit()
                    log("➡ SMS timestamp updated for alarm.")
                except Exception as e:
                    log(f"❌ Failed to update SMS timestamp on alarm: {e}")
                    traceback.print_exc()

                # Also ensure email timestamp is set if we sent email earlier was missing:
                if not email_last_dt and emails:
                    # send email now too (first email missing)
                    subj = f"⚠ {devnm} is Offline - Fertisense Alert"
                    html = offline_html(devnm, diff_minutes)
                    email_sent_now = send_email(subj, html, emails)
                    try:
                        cursor.execute("""
                            UPDATE device_status_alarm_log
                            SET EMAIL_DATE=%s, EMAIL_TIME=%s
                            WHERE DEVICE_STATUS_ALARM_ID=%s
                        """, (now.date() if email_sent_now else None, now.time() if email_sent_now else None, existing_alarm["DEVICE_STATUS_ALARM_ID"]))
                        conn.commit()
                        log("➡ Email timestamp updated for alarm (first email).")
                    except Exception as e:
                        log(f"❌ Failed to update EMAIL timestamp on alarm: {e}")
                        traceback.print_exc()

                continue

            # check 6 hours gap FOR SECOND NOTIFICATION (SMS and Email)
            six_hours_later = sms_last_dt + timedelta(hours=SECOND_NOTIFICATION_HOURS) if sms_last_dt else None

            # Use 'now' (naive) for comparison consistent with sms_last_dt which is naive
            if six_hours_later and now >= six_hours_later:
                log("➡ More than 6 hours since last SMS. Sending repeat SMS + Email.")
                message = build_message(3, devnm)
                sms_sent_any = False
                if phones:
                    for ph in phones:
                        log(f"DEBUG: attempting repeat offline SMS to {ph} -> message: {message[:120]}")
                        if send_sms(ph, message):
                            sms_sent_any = True
                try:
                    cursor.execute("""
                        UPDATE device_status_alarm_log
                        SET SMS_DATE=%s, SMS_TIME=%s
                        WHERE DEVICE_STATUS_ALARM_ID=%s
                    """, (now.date() if sms_sent_any else existing_alarm.get("SMS_DATE"),
                          now.time() if sms_sent_any else existing_alarm.get("SMS_TIME"),
                          existing_alarm["DEVICE_STATUS_ALARM_ID"]))
                    conn.commit()
                    log("➡ Repeated SMS attempt logged.")
                except Exception as e:
                    log(f"❌ Failed to update repeated SMS timestamp on alarm: {e}")
                    traceback.print_exc()

                # also send repeat EMAIL if it makes sense (6 hour rule)
                # We use the same 6-hour gap computed from SMS time for email too (keeps it simple)
                email_sent_any = False
                if emails:
                    subj = f"⚠ {devnm} is Offline - Fertisense Alert"
                    html = offline_html(devnm, diff_minutes)
                    if send_email(subj, html, emails):
                        email_sent_any = True
                try:
                    cursor.execute("""
                        UPDATE device_status_alarm_log
                        SET EMAIL_DATE=%s, EMAIL_TIME=%s
                        WHERE DEVICE_STATUS_ALARM_ID=%s
                    """, (now.date() if email_sent_any else existing_alarm.get("EMAIL_DATE"),
                          now.time() if email_sent_any else existing_alarm.get("EMAIL_TIME"),
                          existing_alarm["DEVICE_STATUS_ALARM_ID"]))
                    conn.commit()
                    log("➡ Repeated EMAIL attempt logged.")
                except Exception as e:
                    log(f"❌ Failed to update repeated EMAIL timestamp on alarm: {e}")
                    traceback.print_exc()
            else:
                log("➡ SMS (and email) already sent recently (<6 hrs). No action.")

        log("✅ All devices processed. Exiting.")

    except Exception as e:
        log(f"❌ Error in check_device_online_status: {e}")
        traceback.print_exc()
    finally:
        try:
            if cursor:
                cursor.close()
        except Exception:
            pass
        try:
            if conn and conn.is_connected():
                conn.close()
        except Exception:
            pass


# ================== RUN ==================
if __name__ == "__main__":
    check_device_online_status()
