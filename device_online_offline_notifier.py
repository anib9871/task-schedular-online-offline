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

OFFLINE_THRESHOLD = 10         # minutes
SECOND_NOTIFICATION_HOURS = 6  # wait 6 hours

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


# =============== DEVICE STATUS ALARM LOG TABLE ===============
# DEVICE_STATUS_ALARM_ID (PK)
# DEVICE_ID
# DEVICE_STATUS (1 active, 0 scrap)
# IS_ACTIVE (1 = offline alarm active, 0 = resolved)
# CREATED_ON_DATE, CREATED_ON_TIME
# UPDATED_ON_DATE, UPDATED_ON_TIME
# SMS_DATE, SMS_TIME
# EMAIL_DATE, EMAIL_TIME


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
            devid = str(device["DEVICE_ID"])
            devnm = device["DEVICE_NAME"]

            # Last reading
            cursor.execute("""
                SELECT READING_DATE, READING_TIME 
                FROM device_reading_log 
                WHERE DEVICE_ID=%s 
                ORDER BY READING_DATE DESC, READING_TIME DESC LIMIT 1
            """, (devid,))
            last_read = cursor.fetchone()

            diff_minutes = None
            if last_read:
                reading_time = last_read["READING_TIME"]
                if isinstance(reading_time, timedelta):
                    total_sec = reading_time.total_seconds()
                    reading_time = dt_time(
                        int(total_sec // 3600),
                        int((total_sec % 3600) // 60),
                        int(total_sec % 60)
                    )
                last_update = datetime.combine(last_read["READING_DATE"], reading_time)
                diff_minutes = (now - last_update).total_seconds() / 60

            current_state = 0 if (diff_minutes is None or diff_minutes > OFFLINE_THRESHOLD) else 1

            # ---- GET LATEST OPEN ALARM ----
            cursor.execute("""
                SELECT * FROM device_status_alarm_log
                WHERE DEVICE_ID=%s AND IS_ACTIVE=1
                ORDER BY DEVICE_STATUS_ALARM_ID DESC LIMIT 1
            """, (devid,))
            existing = cursor.fetchone()

            # ==================================================================
            #                          DEVICE ONLINE
            # ==================================================================
            if current_state == 1:
                print(f"✅ {devnm} is ONLINE")

                if existing:
                    print("➡ Closing open offline alarm & sending ONLINE SMS/Email.")

                    message = build_message(5, devnm)

                    # For now use dummy numbers (you insert your logic here)
                    phones = []
                    emails = []

                    sms_sent = any(send_sms(p, message) for p in phones)
                    email_sent = send_email(f"{devnm} Status Update", message, emails)

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
                        now.date() if sms_sent else existing["SMS_DATE"],
                        now.time() if sms_sent else existing["SMS_TIME"],
                        now.date() if email_sent else existing["EMAIL_DATE"],
                        now.time() if email_sent else existing["EMAIL_TIME"],
                        existing["DEVICE_STATUS_ALARM_ID"]
                    ))
                    conn.commit()
                continue

            # ==================================================================
            #                          DEVICE OFFLINE
            # ==================================================================
            print(f"🚨 {devnm} is OFFLINE")

            # -------- Case A: No active offline alarm → create new --------
            if not existing:
                print("➡ Creating new offline alarm & sending SMS/Email.")

                message = build_message(3, devnm)
                phones = []
                emails = []

                sms_sent = any(send_sms(p, message) for p in phones)
                email_sent = send_email(f"{devnm} Status Update", message, emails)

                cursor.execute("""
                    INSERT INTO device_status_alarm_log
                    (DEVICE_ID, DEVICE_STATUS, IS_ACTIVE,
                     CREATED_ON_DATE, CREATED_ON_TIME,
                     SMS_DATE, SMS_TIME, EMAIL_DATE, EMAIL_TIME)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """, (
                    devid, 1, 1,
                    now.date(), now.time(),
                    now.date() if sms_sent else None,
                    now.time() if sms_sent else None,
                    now.date() if email_sent else None,
                    now.time() if email_sent else None
                ))
                conn.commit()
                print("➕ New offline alarm stored.")
                continue

            # -------- Case B: Offline alarm exists → check 6-hour rule --------
            print("➡ Checking SMS timing for 6-hour rule...")

            sms_last_dt = None
            if existing["SMS_DATE"] and existing["SMS_TIME"]:
                sms_last_dt = datetime.combine(existing["SMS_DATE"], existing["SMS_TIME"])

            # Send if never sent before
            if not sms_last_dt:
                print("➡ First SMS not sent earlier. Sending now.")
                message = build_message(3, devnm)
                phones = []
                sms_sent = any(send_sms(p, message) for p in phones)

                cursor.execute("""
                    UPDATE device_status_alarm_log
                    SET SMS_DATE=%s, SMS_TIME=%s
                    WHERE DEVICE_STATUS_ALARM_ID=%s
                """, (now.date(), now.time(), existing["DEVICE_STATUS_ALARM_ID"]))
                conn.commit()
                continue

            # Check 6-hour gap
            if now >= sms_last_dt + timedelta(hours=SECOND_NOTIFICATION_HOURS):
                print("➡ More than 6 hrs passed. Sending SMS again.")
                message = build_message(3, devnm)
                phones = []
                sms_sent = any(send_sms(p, message) for p in phones)

                cursor.execute("""
                    UPDATE device_status_alarm_log
                    SET SMS_DATE=%s, SMS_TIME=%s
                    WHERE DEVICE_STATUS_ALARM_ID=%s
                """, (now.date(), now.time(), existing["DEVICE_STATUS_ALARM_ID"]))
                conn.commit()
            else:
                print("➡ SMS already sent < 6 hrs. No new SMS.")

        cursor.close()
        conn.close()
        print("✅ Script Completed.")

    except Exception as e:
        print("❌ Error in check_device_online_status:", e)


if __name__ == "__main__":
    check_device_online_status()
