import mysql.connector
import time as t
from datetime import datetime, time as dt_time, timedelta , date
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

OFFLINE_THRESHOLD = 5          # minutes
OFFLINE_VERIFY_MINUTES = 3     # wait before confirming offline
SECOND_NOTIFICATION_HOURS = 6  # wait 6 hours before re-alert

STATE_FILE = "notification_state.json"

# ================== STATE FILE HANDLERS ==================
def load_state():
    """Load notification state from JSON."""
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, "r") as f:
                return json.load(f)
        except json.JSONDecodeError:
            print("⚠️ State file corrupted, resetting.")
            return {}
    return {}

def save_state(state):
    """Save state back to JSON."""
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



def get_contact_info(device_id):
    """Fetch contacts only if device has valid subscription_id=8 and Subcription_End_date >= today."""
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)

        today = date.today()

        # Check subscription with join to get package info
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
        print(f"DEBUG: subscription for device {device_id}:", subscription)

        if not subscription:
            return [], [], 1, 1  # no valid subscription → skip alerts

        # Device info
        cursor.execute("SELECT ORGANIZATION_ID, CENTRE_ID FROM master_device WHERE DEVICE_ID=%s", (device_id,))
        device = cursor.fetchone()
        if not device:
            return [], [], 1, 1

        org_id = device["ORGANIZATION_ID"] or 1
        centre_id = device["CENTRE_ID"] or 1

        # Users linked to org+centre
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

        cursor.execute("SELECT DEVICE_ID, DEVICE_NAME FROM master_device WHERE DEVICE_STATUS = 1")
        devices = cursor.fetchall()
        print(f"✅ Found {len(devices)} active devices")

        # Load previous notification data
        state = load_state()
        print(f"🧾 Loaded {len(state)} records from JSON")

        for device in devices:
            devid = str(device["DEVICE_ID"])  # ensure string keys
            devnm = device["DEVICE_NAME"]

                # --------- CHECK SUBSCRIPTION FIRST ----------
            phones, emails, org_id, centre_id = get_contact_info(devid)
            if not phones and not emails:
                print(f"⏹ {devnm} skipped (no valid subscription)")
                continue  # skip this device entirely

            # Get last reading
            cursor.execute("""
                SELECT READING_DATE, READING_TIME 
                FROM device_reading_log 
                WHERE DEVICE_ID=%s 
                ORDER BY READING_DATE DESC, READING_TIME DESC LIMIT 1
            """, (devid,))
            last_read = cursor.fetchone()
            cursor.fetchall()

            diff_minutes = None
            if last_read:
                reading_time = last_read["READING_TIME"]
                if isinstance(reading_time, timedelta):
                    total_sec = reading_time.total_seconds()
                    reading_time = dt_time(int(total_sec // 3600), int((total_sec % 3600) // 60), int(total_sec % 60))
                last_update = datetime.combine(last_read["READING_DATE"], reading_time)
                diff_minutes = (now - last_update).total_seconds() / 60

            current_state = 0 if (diff_minutes is None or diff_minutes > OFFLINE_THRESHOLD) else 1

            # Verify offline devices again
            if current_state == 0:
                print(f"⚠️ {devnm} appears OFFLINE, verifying...")
                verify_until = datetime.now() + timedelta(minutes=OFFLINE_VERIFY_MINUTES)
                while datetime.now() < verify_until:
                    cursor.execute("""
                        SELECT READING_DATE, READING_TIME 
                        FROM device_reading_log 
                        WHERE DEVICE_ID=%s 
                        ORDER BY READING_DATE DESC, READING_TIME DESC LIMIT 1
                    """, (devid,))
                    last_check = cursor.fetchone()
                    cursor.fetchall()
                    if last_check:
                        reading_time = last_check["READING_TIME"]
                        if isinstance(reading_time, timedelta):
                            total_sec = reading_time.total_seconds()
                            reading_time = dt_time(int(total_sec // 3600), int((total_sec % 3600) // 60), int(total_sec % 60))
                        last_update_check = datetime.combine(last_check["READING_DATE"], reading_time)
                        diff_check = (datetime.now() - last_update_check).total_seconds() / 60
                        if diff_check <= OFFLINE_THRESHOLD:
                            print(f"✅ {devnm} came back online within {OFFLINE_VERIFY_MINUTES} minutes.")
                            current_state = 1
                            break
                    t.sleep(30)

            # ---------------- Notification Logic ----------------
            now_time = datetime.now()
            record = state.get(devid, {})
            last_state = record.get("last_state")
            last_notif_time = record.get("last_notif_time")

            can_notify = False
            reason = ""

            if last_state != current_state:
                can_notify = True
                reason = "State changed"
            elif last_notif_time:
                last_notif_dt = datetime.fromisoformat(last_notif_time)
                if (now_time - last_notif_dt) >= timedelta(hours=SECOND_NOTIFICATION_HOURS):
                    can_notify = True
                    reason = "6-hour reminder"

            if can_notify:
                phones, emails, org_id, centre_id = get_contact_info(devid)
                sms_sent = False
                email_sent = False

                if current_state == 0:
                    print(f"🚨 {devnm} confirmed OFFLINE! Sending alerts. ({reason})")
                    message = build_message(3, devnm)
                else:
                    print(f"✅ {devnm} is ONLINE! Sending info alert. ({reason})")
                    message = build_message(5, devnm)

                for phone in phones:
                    if send_sms(phone, message):
                        sms_sent = True
                email_sent = send_email(f"{devnm} Status Update", message, emails)

                # DB log
                cursor.execute("SELECT id FROM iot_api_devicealarmlog WHERE DEVICE_ID=%s AND ALARM_DATE=%s",
                               (devid, now.date()))
                existing = cursor.fetchone()
                cursor.fetchall()

                if existing:
                    cursor.execute("""
                        UPDATE iot_api_devicealarmlog
                        SET DEVICE_STATUS=%s,
                            DEVICE_STATUS_DATE=%s,
                            DEVICE_STATUS_TIME=%s,
                            DEVICE_STATUS_SMS_DATE=%s,
                            DEVICE_STATUS_SMS_TIME=%s,
                            DEVICE_STATUS_EMAIL_DATE=%s,
                            DEVICE_STATUS_EMAIL_TIME=%s,
                            ORGANIZATION_ID=%s,
                            CENTRE_ID=%s
                        WHERE id=%s
                    """, (
                        current_state,
                        now.date(),
                        now.time(),
                        now.date() if sms_sent else None,
                        now.time() if sms_sent else None,
                        now.date() if email_sent else None,
                        now.time() if email_sent else None,
                        org_id,
                        centre_id,
                        existing['id']
                    ))
                    print(f"📝 Updated alarm log for {devnm}")
                else:
                    cursor.execute("""
                        INSERT INTO iot_api_devicealarmlog
                        (DEVICE_ID, SENSOR_ID, PARAMETER_ID, ALARM_DATE, ALARM_TIME,
                         DEVICE_STATUS, DEVICE_STATUS_DATE, DEVICE_STATUS_TIME,
                         DEVICE_STATUS_SMS_DATE, DEVICE_STATUS_SMS_TIME,
                         DEVICE_STATUS_EMAIL_DATE, DEVICE_STATUS_EMAIL_TIME,
                         ORGANIZATION_ID, CENTRE_ID)
                         VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    """, (
                        devid, 0, 0,
                        now.date(), now.time(),
                        current_state, now.date(), now.time(),
                        now.date() if sms_sent else None, now.time() if sms_sent else None,
                        now.date() if email_sent else None, now.time() if email_sent else None,
                        org_id, centre_id
                    ))
                    print(f"➕ Inserted new alarm log for {devnm}")

                conn.commit()

                # ✅ Update state file
                state[devid] = {
                    "last_state": current_state,
                    "last_notif_time": now_time.isoformat()
                }
                save_state(state)
                print(f"💾 State updated for {devnm}")

            else:
                print(f"⏳ {devnm} skipped (same state, no cooldown reached).")

        cursor.close()
        conn.close()
        print("✅ Done... Ending Script.")
    except Exception as e:
        print("❌ Error in check_device_online_status:", e)

# ================== RUN ==================
if __name__ == "__main__":
    check_device_online_status()
