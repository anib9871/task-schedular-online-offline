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

# NEW: use 10 minutes as requested
OFFLINE_THRESHOLD = 10         # minutes
SECOND_NOTIFICATION_HOURS = 6  # wait 6 hours before re-alert

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
        cursor.execute("SELECT ORGANIZATION_ID, CENTRE_ID FROM iot_api_masterdevice WHERE DEVICE_ID=%s", (device_id,))
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

        cursor.execute("SELECT DEVICE_ID, DEVICE_NAME FROM iot_api_masterdevice WHERE DEVICE_STATUS = 1")
        devices = cursor.fetchall()
        print(f"✅ Found {len(devices)} active devices")

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

            # NEW: Determine online/offline using 10-minute threshold, NO verification loop
            current_state = 0 if (diff_minutes is None or diff_minutes > OFFLINE_THRESHOLD) else 1

            # ---------------- Notification Logic (DB-driven, no JSON) ----------------
            # Find any existing OPEN offline alarm (no date filter)
            cursor.execute("""
                SELECT * FROM iot_api_devicealarmlog
                WHERE DEVICE_ID=%s AND DEVICE_ONLINE_STATUS=0
                ORDER BY id DESC LIMIT 1
            """, (devid,))
            existing_offline = cursor.fetchone()
            cursor.fetchall()

            if current_state == 1:
                # DEVICE ONLINE
                print(f"✅ {devnm} is ONLINE")
                # If offline alarm exists (open), send ONLINE notification and close it
                if existing_offline:
                    print("➡ Found open offline alarm. Sending ONLINE SMS/Email and closing alarm.")
                    message = build_message(5, devnm)
                    sms_sent = False
                    email_sent = False

                    for phone in phones:
                        if send_sms(phone, message):
                            sms_sent = True
                    email_sent = send_email(f"{devnm} Status Update", message, emails)

                    # Update the existing alarm: mark online, update times and org/centre
                    cursor.execute("""
                        UPDATE iot_api_devicealarmlog
                        SET DEVICE_ONLINE_STATUS=%s,
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
                        1,
                        now.date(),
                        now.time(),
                        now.date() if sms_sent else existing_offline.get('DEVICE_STATUS_SMS_DATE'),
                        now.time() if sms_sent else existing_offline.get('DEVICE_STATUS_SMS_TIME'),
                        now.date() if email_sent else existing_offline.get('DEVICE_STATUS_EMAIL_DATE'),
                        now.time() if email_sent else existing_offline.get('DEVICE_STATUS_EMAIL_TIME'),
                        org_id,
                        centre_id,
                        existing_offline['id']
                    ))
                    conn.commit()
                else:
                    # No open offline alarm and online — nothing to do
                    print("➡ No open offline alarm. Nothing to update.")
                continue  # next device

            # current_state == 0 => DEVICE OFFLINE
            print(f"🚨 {devnm} is OFFLINE")

            if not existing_offline:
                # Case A: No offline entry exists -> create new and send SMS
                print("➡ No offline alarm exists. Creating new offline alarm and sending SMS/Email.")
                message = build_message(3, devnm)
                sms_sent = False
                email_sent = False

                for phone in phones:
                    if send_sms(phone, message):
                        sms_sent = True
                email_sent = send_email(f"{devnm} Status Update", message, emails)

                cursor.execute("""
                    INSERT INTO iot_api_devicealarmlog
                    (DEVICE_ID, SENSOR_ID, PARAMETER_ID, ALARM_DATE, ALARM_TIME,
                     DEVICE_ONLINE_STATUS, DEVICE_STATUS_DATE, DEVICE_STATUS_TIME,
                     DEVICE_STATUS_SMS_DATE, DEVICE_STATUS_SMS_TIME,
                     DEVICE_STATUS_EMAIL_DATE, DEVICE_STATUS_EMAIL_TIME,
                     ORGANIZATION_ID, CENTRE_ID)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """, (
                    devid, 0, 0,
                    now.date(), now.time(),
                    0, now.date(), now.time(),
                    now.date() if sms_sent else None, now.time() if sms_sent else None,
                    now.date() if email_sent else None, now.time() if email_sent else None,
                    org_id, centre_id
                ))
                conn.commit()
                print("➕ Inserted new offline alarm.")
                continue

            # Case B: Offline entry exists -> check SMS sent time and 6-hour rule
            print("➡ Offline alarm already exists. Checking SMS timing rules.")
            sms_date = existing_offline.get("DEVICE_STATUS_SMS_DATE")
            sms_time = existing_offline.get("DEVICE_STATUS_SMS_TIME")
            sms_last_sent_dt = None
            if sms_date and sms_time:
                try:
                    # sms_time could be stored as time object
                    sms_last_sent_dt = datetime.combine(sms_date, sms_time) if isinstance(sms_date, date) else None
                except Exception:
                    sms_last_sent_dt = None

            if not sms_last_sent_dt:
                # SMS never sent for this alarm — send now
                print("➡ SMS not sent previously. Sending SMS now.")
                message = build_message(3, devnm)
                sms_sent = False
                for phone in phones:
                    if send_sms(phone, message):
                        sms_sent = True
                # update sms date/time in alarm row
                cursor.execute("""
                    UPDATE iot_api_devicealarmlog
                    SET DEVICE_STATUS_SMS_DATE=%s, DEVICE_STATUS_SMS_TIME=%s, ORGANIZATION_ID=%s, CENTRE_ID=%s
                    WHERE id=%s
                """, (now.date() if sms_sent else None, now.time() if sms_sent else None, org_id, centre_id, existing_offline['id']))
                conn.commit()
                continue

            # If SMS was sent earlier -> check 6-hour gap
            six_hours_after = sms_last_sent_dt + timedelta(hours=SECOND_NOTIFICATION_HOURS)
            if datetime.now() >= six_hours_after:
                print("➡ More than 6 hours passed since last SMS. Sending SMS again.")
                message = build_message(3, devnm)
                sms_sent = False
                for phone in phones:
                    if send_sms(phone, message):
                        sms_sent = True
                cursor.execute("""
                    UPDATE iot_api_devicealarmlog
                    SET DEVICE_STATUS_SMS_DATE=%s, DEVICE_STATUS_SMS_TIME=%s, ORGANIZATION_ID=%s, CENTRE_ID=%s
                    WHERE id=%s
                """, (now.date() if sms_sent else existing_offline.get('DEVICE_STATUS_SMS_DATE'),
                      now.time() if sms_sent else existing_offline.get('DEVICE_STATUS_SMS_TIME'),
                      org_id, centre_id, existing_offline['id']))
                conn.commit()
            else:
                print("➡ SMS was sent recently (<6 hrs). No action needed.")

        cursor.close()
        conn.close()
        print("✅ Done... Ending Script.")
    except Exception as e:
        print("❌ Error in check_device_online_status:", e)

# ================== RUN ==================
if __name__ == "__main__":
    check_device_online_status()
