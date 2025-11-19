import mysql.connector
import time as t
from datetime import datetime, time as dt_time, timedelta, date
import requests
import smtplib
from email.mime.text import MIMEText
import json, os

# ================== CONFIG ==================
db_config = {
    "host": "switchback.proxy.rlwy.net",
    "user": "root",
    "port": 44750,
    "password": "qYxlhEiaEvtiRvKaFyigDPtXSSCpddMv",
    "database": "railway",
}

SMS_API_URL = "http://www.universalsmsadvertising.com/universalsmsapi.php"
SMS_USER = "8960853914"
SMS_PASS = "8960853914"
SENDER_ID = "FRTLLP"

SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587
EMAIL_USER = "testwebservice71@gmail.com"
EMAIL_PASS = "akuu vulg ejlg ysbt"

OFFLINE_THRESHOLD = 5          # minutes (device considered offline if last reading older than this)
SECOND_NOTIFICATION_HOURS = 6  # resend periodic reminder after this many hours

STATE_FILE = "notification_state.json"

# ================== STATE FILE HANDLERS ==================
def load_state():
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, "r") as f:
                return json.load(f)
        except json.JSONDecodeError:
            print("⚠️ State file corrupted, resetting.")
            return {}
    return {}

def save_state(state):
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)

# ================== HELPERS ==================
def build_message(ntf_typ, devnm):
    messages = {
        3: f"WARNING!! The {devnm} is offline. Please take necessary action - Regards Fertisense LLP",
        5: f"INFO!! The device {devnm} is back online. No action is required - Regards Fertisense LLP",
    }
    return messages.get(ntf_typ, f"Alert for {devnm} - Regards Fertisense LLP")

def send_sms(phone, message):
    try:
        params = {
            "user_name": SMS_USER,
            "user_password": SMS_PASS,
            "mobile": phone,
            "sender_id": SENDER_ID,
            "type": "F",
            "text": message
        }
        requests.get(SMS_API_URL, params=params, timeout=10)
        print(f"✅ SMS sent: {phone}")
        return True
    except Exception as e:
        print("❌ SMS failed:", e)
        return False

def send_email(subject, message, email_ids):
    if not email_ids:
        return False
    try:
        msg = MIMEText(message)
        msg["Subject"] = subject
        msg["From"] = EMAIL_USER
        msg["To"] = ", ".join(email_ids)
        server = smtplib.SMTP(SMTP_SERVER, SMTP_PORT)
        server.starttls()
        server.login(EMAIL_USER, EMAIL_PASS)
        server.sendmail(EMAIL_USER, email_ids, msg.as_string())
        server.quit()
        print("✅ Email sent:", subject)
        return True
    except Exception as e:
        print("❌ Email failed:", e)
        return False

# ================== CONTACT FETCH ==================
def get_contact_info(device_id):
    """Fetch contacts only if device has valid subscription_id=8 and Subcription_End_date >= today."""
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)

        today = date.today()

        cursor.execute("""
            SELECT sh.*, msi.Package_Name
            FROM Subcription_History sh
            JOIN Master_Subscription_Info msi
              ON sh.Subscription_ID = msi.Subscription_ID
            WHERE sh.Device_ID=%s
              AND sh.Subscription_ID=8
              AND sh.Subcription_End_date >= %s
        """, (device_id, today))
        subscription = cursor.fetchone()

        # Debug
        # print(f"DEBUG: subscription for device {device_id}:", subscription)

        if not subscription:
            return [], [], 1, 1  # no valid subscription → skip alerts

        cursor.execute("SELECT ORGANIZATION_ID, CENTRE_ID FROM master_device WHERE DEVICE_ID=%s", (device_id,))
        device = cursor.fetchone()
        if not device:
            return [], [], 1, 1

        org_id = device["ORGANIZATION_ID"] or 1
        centre_id = device["CENTRE_ID"] or 1

        cursor.execute("""
            SELECT USER_ID_id FROM userorganizationcentrelink 
            WHERE ORGANIZATION_ID_id=%s AND CENTRE_ID_id=%s
        """, (org_id, centre_id))
        user_ids = [u["USER_ID_id"] for u in cursor.fetchall()]
        if not user_ids:
            return [], [], org_id, centre_id

        format_strings = ','.join(['%s']*len(user_ids))
        cursor.execute(f"""
            SELECT PHONE, EMAIL, SEND_SMS, SEND_EMAIL
            FROM master_user 
            WHERE USER_ID IN ({format_strings})
              AND (SEND_SMS=1 OR SEND_EMAIL=1)
        """, tuple(user_ids))
        users = cursor.fetchall()

        phones = [u["PHONE"] for u in users if u["SEND_SMS"] == 1]
        emails = [u["EMAIL"] for u in users if u["SEND_EMAIL"] == 1]
        return phones, emails, org_id, centre_id

    except Exception as e:
        print("❌ Error getting contacts:", e)
        return [], [], 1, 1
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals() and conn.is_connected():
            conn.close()

# ================== MAIN LOGIC ==================
def check_device_online_status():
    try:
        print("🚀 Starting Script...")
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)
        now = datetime.now()

        # Only monitor devices that are not condemned (DEVICE_STATUS = 1)
        cursor.execute("SELECT DEVICE_ID, DEVICE_NAME FROM master_device WHERE DEVICE_STATUS = 1")
        devices = cursor.fetchall()
        print(f"✅ Found {len(devices)} active devices")

        # Load previous notification data
        state = load_state()
        print(f"🧾 Loaded {len(state)} records from JSON")

        for device in devices:
            devid = str(device["DEVICE_ID"])  # string key for JSON
            devnm = device["DEVICE_NAME"]

            # --------- CHECK SUBSCRIPTION / CONTACTS ----------
            phones, emails, org_id, centre_id = get_contact_info(devid)
            if not phones and not emails:
                print(f"⏹ {devnm} skipped (no valid subscription / contacts)")
                continue  # skip this device entirely

            # Get last reading
            cursor.execute("""
                SELECT READING_DATE, READING_TIME 
                FROM device_reading_log 
                WHERE DEVICE_ID=%s 
                ORDER BY READING_DATE DESC, READING_TIME DESC LIMIT 1
            """, (devid,))
            last_read = cursor.fetchone()

            # compute difference in minutes
            diff_minutes = None
            if last_read:
                reading_time = last_read["READING_TIME"]
                # handle TIME stored as INTERVAL/timedelta
                if isinstance(reading_time, timedelta):
                    total_sec = reading_time.total_seconds()
                    reading_time = dt_time(int(total_sec // 3600), int((total_sec % 3600) // 60), int(total_sec % 60))
                try:
                    last_update = datetime.combine(last_read["READING_DATE"], reading_time)
                    diff_minutes = (now - last_update).total_seconds() / 60
                except Exception as e:
                    # any bad data → treat as no reading
                    print(f"⚠️ {devnm} reading parse error:", e)
                    diff_minutes = None

            # Determine online (1) or offline (0)
            # If no reading or older than threshold => offline
            current_state = 0 if (diff_minutes is None or diff_minutes > OFFLINE_THRESHOLD or diff_minutes < 0) else 1

            # ---------- Notification decision ----------
            now_time = now
            record = state.get(devid, {})
            last_state = record.get("last_state")  # could be None or int
            last_notif_time = record.get("last_notif_time")  # isoformat string

            can_notify = False
            reason = ""

            # 1) If state changed from last_state -> notify (first notification on change)
            if last_state is None:
                # first time we see this device in state file:
                # - if current_state == 0 => send first OFFLINE notification
                # - if current_state == 1 => suppress initial ONLINE notification to avoid false positives
                if current_state == 0:
                    can_notify = True
                    reason = "initial-offline"
                else:
                    can_notify = False
                    reason = "initial-online-suppressed"
            elif int(last_state) != int(current_state):
                can_notify = True
                reason = "state-changed"
            else:
                # 2) If same state and we have last_notif_time, allow periodic reminders after SECOND_NOTIFICATION_HOURS
                if last_notif_time:
                    try:
                        last_notif_dt = datetime.fromisoformat(last_notif_time)
                        if (now_time - last_notif_dt) >= timedelta(hours=SECOND_NOTIFICATION_HOURS):
                            can_notify = True
                            reason = "periodic-reminder"
                    except Exception:
                        # corrupted time -> allow notify and reset
                        can_notify = True
                        reason = "periodic-badtime"

            if can_notify:
                # Re-fetch contacts (in case changed)
                phones, emails, org_id, centre_id = get_contact_info(devid)
                sms_sent = False
                email_sent = False

                if current_state == 0:
                    print(f"🚨 {devnm} confirmed OFFLINE! Sending alerts. ({reason})")
                    message = build_message(3, devnm)
                else:
                    print(f"✅ {devnm} is ONLINE! Sending info alert. ({reason})")
                    message = build_message(5, devnm)

                # send SMSs
                for phone in phones:
                    if send_sms(phone, message):
                        sms_sent = True

                # send Email
                email_sent = send_email(f"{devnm} Status Update", message, emails)

                # ---------------- DB log (INSERT or UPDATE) ----------------
                # Find today's alarm record if exists (match by DEVICE_ID and ALARM_DATE)
                cursor.execute("SELECT * FROM iot_api_devicealarmlog WHERE DEVICE_ID=%s AND ALARM_DATE=%s ORDER BY id DESC LIMIT 1",
                               (devid, now.date()))
                existing = cursor.fetchone()

                sms_date = now.date() if sms_sent else None
                sms_time = now.time() if sms_sent else None
                email_date = now.date() if email_sent else None
                email_time = now.time() if email_sent else None

                if existing:
                    # Update existing alarm row: set DEVICE_ONLINE_STATUS and timestamp columns
                    cursor.execute("""
                        UPDATE iot_api_devicealarmlog
                        SET
                            DEVICE_ONLINE_STATUS=%s,
                            DEVICE_STATUS=1,
                            DEVICE_STATUS_DATE=%s,
                            DEVICE_STATUS_TIME=%s,
                            DEVICE_STATUS_SMS_DATE=%s,
                            DEVICE_STATUS_SMS_TIME=%s,
                            DEVICE_STATUS_EMAIL_DATE=%s,
                            DEVICE_STATUS_EMAIL_TIME=%s,
                            ORGANIZATION_ID=%s,
                            CENTRE_ID=%s,
                            LST_UPD_DT=%s
                        WHERE id=%s
                    """, (
                        current_state,
                        now.date(), now.time(),
                        sms_date, sms_time,
                        email_date, email_time,
                        org_id, centre_id,
                        now.date(),
                        existing["id"]
                    ))
                    print(f"📝 Updated alarm log for {devnm} (id={existing['id']})")
                else:
                    # Insert new alarm row for today
                    cursor.execute("""
                        INSERT INTO iot_api_devicealarmlog
                        (DEVICE_ID, SENSOR_ID, PARAMETER_ID,
                         ALARM_DATE, ALARM_TIME,
                         DEVICE_ONLINE_STATUS,
                         DEVICE_STATUS, DEVICE_STATUS_DATE, DEVICE_STATUS_TIME,
                         DEVICE_STATUS_SMS_DATE, DEVICE_STATUS_SMS_TIME,
                         DEVICE_STATUS_EMAIL_DATE, DEVICE_STATUS_EMAIL_TIME,
                         ORGANIZATION_ID, CENTRE_ID,
                         CRT_DT, IS_ACTIVE)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    """, (
                        devid, 0, 0,
                        now.date(), now.time(),
                        current_state,
                        1, now.date(), now.time(),
                        sms_date, sms_time,
                        email_date, email_time,
                        org_id, centre_id,
                        now.date(), 1
                    ))
                    print(f"➕ Inserted new alarm log for {devnm}")

                conn.commit()

                # Update JSON state
                state[devid] = {
                    "last_state": current_state,
                    "last_notif_time": now_time.isoformat()
                }
                save_state(state)
                print(f"💾 State updated for {devnm}")
            else:
                # no notification now; ensure we have a baseline last_state stored to avoid initial re-notify loops
                if devid not in state:
                    state[devid] = {
                        "last_state": current_state,
                        "last_notif_time": now_time.isoformat()
                    }
                    save_state(state)
                print(f"⏳ {devnm} skipped (no notify). State={current_state}")

        cursor.close()
        conn.close()
        print("✅ Done... Ending Script.")
    except Exception as e:
        print("❌ Error in check_device_online_status:", e)


# ================== RUN ==================
if __name__ == "__main__":
    check_device_online_status()
