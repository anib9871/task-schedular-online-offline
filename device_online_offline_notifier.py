import mysql.connector
import time as t  # for sleep
from datetime import datetime, time as dt_time, timedelta
import requests
import smtplib
from email.mime.text import MIMEText

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
SECOND_NOTIFICATION_HOURS = 6  # wait 6 hours before 2nd notification

# ================== TRACK STATE ==================
device_status = {}                # DEVICE_ID -> online/offline
device_notifications = {}         # DEVICE_ID -> count of notifications sent
device_last_notification = {}     # DEVICE_ID -> datetime of last notification

# ================== FUNCTIONS ==================
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
        print("✅ SMS sent:", phone, message)
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
        print("✅ Email sent:", message)
        return True
    except Exception as e:
        print("❌ Email failed:", e)
        return False

def get_contact_info(device_id):
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)

        cursor.execute("SELECT ORGANIZATION_ID, CENTRE_ID FROM master_device WHERE DEVICE_ID=%s", (device_id,))
        device = cursor.fetchone()
        if not device:
            return [], [], 1, 1

        org_id = device["ORGANIZATION_ID"] or 1
        centre_id = device["CENTRE_ID"] or 1

        cursor.execute("SELECT USER_ID_id FROM userorganizationcentrelink WHERE ORGANIZATION_ID_id=%s AND CENTRE_ID_id=%s",
                       (org_id, centre_id))
        user_ids = [u["USER_ID_id"] for u in cursor.fetchall()]
        if not user_ids:
            return [], [], org_id, centre_id

        format_strings = ','.join(['%s']*len(user_ids))
        cursor.execute(f"SELECT PHONE, EMAIL, SEND_SMS, SEND_EMAIL FROM master_user WHERE USER_ID IN ({format_strings}) AND (SEND_SMS=1 OR SEND_EMAIL=1)",
                       tuple(user_ids))
        users = cursor.fetchall()
        phones = [u["PHONE"] for u in users if u["SEND_SMS"]==1]
        emails = [u["EMAIL"] for u in users if u["SEND_EMAIL"]==1]
        return phones, emails, org_id, centre_id
    except Exception as e:
        print("❌ Error getting contacts:", e)
        return [], [], 1, 1
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals() and conn.is_connected():
            conn.close()

# ================== MAIN CHECK ==================
def check_device_online_status():
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)
        now = datetime.now()

        # ✅ Fetch only active devices
        cursor.execute("SELECT DEVICE_ID, DEVICE_NAME FROM master_device WHERE DEVICE_STATUS = 1")
        devices = cursor.fetchall()

        for device in devices:
            devid = device["DEVICE_ID"]
            devnm = device["DEVICE_NAME"]
            ...
            # rest of your logic remains same

            # ---- get last reading ----
            cursor.execute(
                "SELECT READING_DATE, READING_TIME FROM device_reading_log WHERE DEVICE_ID=%s ORDER BY READING_DATE DESC, READING_TIME DESC LIMIT 1",
                (devid,)
            )
            last_read = cursor.fetchone()
            diff_minutes = None
            if last_read:
                reading_time = last_read["READING_TIME"]
                if isinstance(reading_time, timedelta):
                    total_sec = reading_time.total_seconds()
                    reading_time = dt_time(int(total_sec // 3600), int((total_sec % 3600) // 60), int(total_sec % 60))
                last_update = datetime.combine(last_read["READING_DATE"], reading_time)
                diff_minutes = (now - last_update).total_seconds() / 60

            current_state = 0 if (diff_minutes is None or diff_minutes > OFFLINE_THRESHOLD) else 1
            previous_state = device_status.get(devid)

            if previous_state != current_state:
                if devid not in device_notifications:
                    device_notifications[devid] = 0

                # ✅ Verification delay logic before confirming offline
                if current_state == 0:
                    print(f"⚠️ {devnm} appears OFFLINE, verifying for {OFFLINE_VERIFY_MINUTES} minutes...")
                    verify_until = datetime.now() + timedelta(minutes=OFFLINE_VERIFY_MINUTES)
                    while datetime.now() < verify_until:
                        cursor.execute(
                            "SELECT READING_DATE, READING_TIME FROM device_reading_log WHERE DEVICE_ID=%s ORDER BY READING_DATE DESC, READING_TIME DESC LIMIT 1",
                            (devid,)
                        )
                        last_check = cursor.fetchone()
                        if last_check:
                            reading_time = last_check["READING_TIME"]
                            if isinstance(reading_time, timedelta):
                                total_sec = reading_time.total_seconds()
                                reading_time = dt_time(int(total_sec // 3600), int((total_sec % 3600) // 60), int(total_sec % 60))
                            last_update_check = datetime.combine(last_check["READING_DATE"], reading_time)
                            diff_check = (datetime.now() - last_update_check).total_seconds() / 60
                            if diff_check <= OFFLINE_THRESHOLD:
                                print(f"✅ {devnm} came back online within verification window, no alert sent.")
                                current_state = 1
                                break
                        t.sleep(30)

                # ---- Final state check ----
                if previous_state != current_state:
                    now_time = datetime.now()
                    can_notify = False

                    # check first notification or 6-hour gap
                    if devid not in device_last_notification:
                        can_notify = True
                    else:
                        last_notif = device_last_notification[devid]
                        if (now_time - last_notif) >= timedelta(hours=SECOND_NOTIFICATION_HOURS):
                            can_notify = True

                    if can_notify:
                        device_status[devid] = current_state
                        device_last_notification[devid] = now_time
                        device_notifications[devid] = device_notifications.get(devid, 0) + 1

                        phones, emails, org_id, centre_id = get_contact_info(devid)
                        sms_sent = False
                        email_sent = False

                        if current_state == 0:
                            print(f"🚨 {devnm} confirmed OFFLINE! Sending alerts.")
                            message = build_message(3, devnm)
                        else:
                            print(f"✅ {devnm} is ONLINE! Sending info alert.")
                            message = build_message(5, devnm)

                        # ---- send sms ----
                        for phone in phones:
                            if send_sms(phone, message):
                                sms_sent = True

                        # ---- send email ----
                        email_sent = send_email(f"{devnm} Status Update", message, emails)

                        # ---- update DB ----
                        cursor.execute("SELECT id FROM iot_api_devicealarmlog WHERE DEVICE_ID=%s AND ALARM_DATE=%s",
                                       (devid, now.date()))
                        existing = cursor.fetchone()

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
                                 IS_ACTIVE, ORGANIZATION_ID, CENTRE_ID)
                                 VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,1,%s,%s)
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
                    else:
                        print(f"⚠️ {devnm} state changed but 6-hour notification limit not reached.")
                        device_status[devid] = current_state
            else:
                print(f"⏹ No state change for {devnm}, current state: {current_state}")

        cursor.close()
        conn.close()

    except Exception as e:
        print("❌ Error in check_device_online_status:", e)

# ================== RUN LOOP ==================
if __name__ == "__main__":
        print("🚀 Starting Devices check...")
        check_device_online_status()
        print("✅ Devices check complete. Exiting now.")
