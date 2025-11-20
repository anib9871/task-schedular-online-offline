import mysql.connector
from datetime import datetime, time as dt_time, timedelta, date
import requests
import smtplib
from email.mime.text import MIMEText
import os
import sys
import traceback

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

SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587
EMAIL_USER = "testwebservice71@gmail.com"
EMAIL_PASS = "akuu vulg ejlg ysbt"

OFFLINE_THRESHOLD = 5         # minutes
SECOND_NOTIFICATION_HOURS = 6  # hours between repeated offline notifications

# ================== HELPERS ==================
def log(msg):
    print(f"[{datetime.now().isoformat(sep=' ', timespec='seconds')}] {msg}")

def build_message(ntf_typ, devnm):
    messages = {
        3: f"WARNING!! The {devnm} is offline. Please take necessary action - Regards Fertisense LLP",
        5: f"INFO!! The device {devnm} is back online. No action is required - Regards Fertisense LLP",
    }
    return messages.get(ntf_typ, f"Alert for {devnm} - Regards Fertisense LLP")

def send_sms(phone, message):
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
        r = requests.get(SMS_API_URL, params=params, timeout=15)
        log(f"SMS API -> phone={phone} status_code={r.status_code} text={r.text[:200]}")
        return r.status_code == 200
    except Exception as e:
        log(f"❌ SMS failed for {phone}: {e}")
        return False

def send_email(subject, message, email_ids):
    if not email_ids:
        return False
    try:
        msg = MIMEText(message)
        msg["Subject"] = subject
        msg["From"] = EMAIL_USER
        msg["To"] = ", ".join(email_ids)
        server = smtplib.SMTP(SMTP_SERVER, SMTP_PORT, timeout=20)
        server.starttls()
        server.login(EMAIL_USER, EMAIL_PASS)
        server.sendmail(EMAIL_USER, email_ids, msg.as_string())
        server.quit()
        log(f"✅ Email sent to {len(email_ids)} recipients")
        return True
    except Exception as e:
        log(f"❌ Email failed: {e}")
        return False

# Fetch contacts IF subscription valid
def get_contact_info(device_id):
    """Return (phones_list, emails_list, org_id, centre_id)
    If subscription invalid -> returns ([], [], org_id, centre_id) or ([], [], 1, 1)
    """
    conn = None
    cursor = None
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)

        today = date.today()

        # Subscription check (uses your existing logic: Subscription_ID = 8)
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
            # Some rows might have column names slightly different; try safe access
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

# ================== MAIN LOGIC ==================
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
    # string "HH:MM:SS"
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

def check_device_online_status():
    conn = None
    cursor = None
    try:
        log("🚀 Starting device online/offline check")
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)

        now = datetime.now()

        cursor.execute("SELECT DEVICE_ID, DEVICE_NAME FROM iot_api_masterdevice WHERE DEVICE_STATUS = 1")
        devices = cursor.fetchall()
        log(f"✅ Found {len(devices)} active devices")

        for d in devices:
            devid = d["DEVICE_ID"]
            devnm = d.get("DEVICE_NAME") or f"Device-{devid}"
            log(f"\n--- Processing device {devid} : {devnm} ---")

            # subscription + contacts
            phones, emails, org_id, centre_id = get_contact_info(devid)
            log(f"DEBUG contacts -> phones={phones} emails={emails} org={org_id} centre={centre_id}")
            if not phones and not emails:
                log(f"⏹ {devnm} skipped (no valid subscription or no contacts)")
                continue

            # last reading
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
                rt = parse_reading_time(last_read.get("READING_TIME"))
                rd = last_read.get("READING_DATE")
                if rt and rd:
                    last_update = datetime.combine(rd, rt)
                    diff_minutes = (now - last_update).total_seconds() / 60.0
                    log(f"DEBUG last_read -> date={rd} time={rt} last_update={last_update} diff_min={diff_minutes:.1f}")
                else:
                    log(f"DEBUG last_read present but could not parse READING_TIME: {last_read.get('READING_TIME')}")
            else:
                log("DEBUG no readings found for device; treating as offline")

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

                    sms_sent_any = False
                    if phones:
                        for ph in phones:
                            if send_sms(ph, message):
                                sms_sent_any = True
                    email_sent = send_email(f"{devnm} Status Update", message, emails) if emails else False

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
                else:
                    log("➡ No open alarm; nothing to do.")
                continue

            # ---------- DEVICE OFFLINE ----------
            log(f"🚨 {devnm} is OFFLINE (diff_min={'NA' if diff_minutes is None else f'{diff_minutes:.1f}'})")

            # Case A: create new alarm if none
            if not existing_alarm:
                log("➡ No active alarm exists. Creating new offline alarm and sending initial notifications.")
                message = build_message(3, devnm)
                sms_sent_any = False
                if phones:
                    for ph in phones:
                        if send_sms(ph, message):
                            sms_sent_any = True
                email_sent = send_email(f"{devnm} Status Update", message, emails) if emails else False

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
                continue

            # Case B: existing offline alarm -> handle SMS timing (6-hour rule)
            log("➡ Active offline alarm exists. Checking SMS timing rules.")

            sms_date = existing_alarm.get("SMS_DATE")
            sms_time = existing_alarm.get("SMS_TIME")
            sms_last_dt = None
            try:
                if sms_date and sms_time:
                    sms_last_dt = datetime.combine(sms_date, sms_time)
            except Exception:
                sms_last_dt = None

            if not sms_last_dt:
                log("➡ No SMS sent previously for this alarm. Sending now.")
                message = build_message(3, devnm)
                sms_sent_any = False
                if phones:
                    for ph in phones:
                        if send_sms(ph, message):
                            sms_sent_any = True
                cursor.execute("""
                    UPDATE device_status_alarm_log
                    SET SMS_DATE=%s, SMS_TIME=%s
                    WHERE DEVICE_STATUS_ALARM_ID=%s
                """, (now.date() if sms_sent_any else None, now.time() if sms_sent_any else None, existing_alarm["DEVICE_STATUS_ALARM_ID"]))
                conn.commit()
                log("➡ SMS timestamp updated for alarm.")
                continue

            # check 6 hours gap
            if datetime.now() >= sms_last_dt + timedelta(hours=SECOND_NOTIFICATION_HOURS):
                log("➡ More than 6 hours since last SMS. Sending repeat SMS.")
                message = build_message(3, devnm)
                sms_sent_any = False
                if phones:
                    for ph in phones:
                        if send_sms(ph, message):
                            sms_sent_any = True
                cursor.execute("""
                    UPDATE device_status_alarm_log
                    SET SMS_DATE=%s, SMS_TIME=%s
                    WHERE DEVICE_STATUS_ALARM_ID=%s
                """, (now.date() if sms_sent_any else existing_alarm.get("SMS_DATE"),
                      now.time() if sms_sent_any else existing_alarm.get("SMS_TIME"),
                      existing_alarm["DEVICE_STATUS_ALARM_ID"]))
                conn.commit()
                log("➡ Repeated SMS attempt logged.")
            else:
                log("➡ SMS already sent recently (<6 hrs). No action.")

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
