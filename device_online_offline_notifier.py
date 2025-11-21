#!/usr/bin/env python3
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

SMS_API_URL = "http://www.universalsmsadvertising.com/universalsmsapi.php"
SMS_USER = "8960853914"
SMS_PASS = "8960853914"
SENDER_ID = "FRTLLP"

SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587
EMAIL_USER = "testwebservice71@gmail.com"
EMAIL_PASS = "akuu vulg ejlg ysbt"

OFFLINE_THRESHOLD = 5       # minutes
SECOND_NOTIFICATION_HOURS = 6  # hours

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
        r = requests.get(SMS_API_URL, params=params, timeout=30)
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
        log("✅ Email sent")
        return True
    except Exception as e:
        log(f"❌ Email failed: {e}")
        return False

# Subscription/contact fetch
def get_contact_info(device_id):
    conn = None
    cursor = None
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
            ORDER BY sh.Subcription_End_date DESC
            LIMIT 1
        """, (device_id, today))
        subscription = cursor.fetchone()

        if not subscription:
            return [], [], 1, 1

        cursor.execute("SELECT ORGANIZATION_ID, CENTRE_ID FROM iot_api_masterdevice WHERE DEVICE_ID=%s", (device_id,))
        device = cursor.fetchone()
        if not device:
            return [], [], 1, 1

        org_id = device["ORGANIZATION_ID"]
        centre_id = device["CENTRE_ID"]

        cursor.execute("""
            SELECT USER_ID_id FROM userorganizationcentrelink
            WHERE ORGANIZATION_ID_id=%s AND CENTRE_ID_id=%s
        """, (org_id, centre_id))
        rows = cursor.fetchall()
        user_ids = [r["USER_ID_id"] for r in rows]

        if not user_ids:
            return [], [], org_id, centre_id

        format_str = ",".join(["%s"] * len(user_ids))
        cursor.execute(f"""
            SELECT USER_ID, PHONE, EMAIL, SEND_SMS, SEND_EMAIL
            FROM master_user
            WHERE USER_ID IN ({format_str})
        """, tuple(user_ids))
        users = cursor.fetchall()

        phones = []
        emails = []

        for u in users:
            if u["SEND_SMS"] == 1 and u["PHONE"]:
                phones.append(str(u["PHONE"]))
            if u["SEND_EMAIL"] == 1 and u["EMAIL"]:
                emails.append(u["EMAIL"])

        phones = list(dict.fromkeys(phones))
        emails = list(dict.fromkeys(emails))

        return phones, emails, org_id, centre_id

    except:
        return [], [], 1, 1
    finally:
        if cursor: cursor.close()
        if conn and conn.is_connected(): conn.close()

# Parse reading time
def parse_reading_time(val):
    if val is None:
        return None
    if isinstance(val, timedelta):
        sec = int(val.total_seconds())
        return dt_time(sec // 3600, (sec % 3600) // 60, sec % 60)
    try:
        if hasattr(val, "hour"):
            return val
    except:
        pass
    if isinstance(val, str):
        p = val.split(":")
        if len(p) == 3:
            return dt_time(int(p[0]), int(p[1]), int(p[2]))
    return None

# ================== MAIN ==================
def check_device_online_status():
    conn = None
    cursor = None
    try:
        log("🚀 Starting device online/offline check")
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)

        now = datetime.now()

        cursor.execute("SELECT DEVICE_ID, DEVICE_NAME FROM iot_api_masterdevice WHERE DEVICE_STATUS=1")
        devices = cursor.fetchall()

        for d in devices:
            devid = d["DEVICE_ID"]
            devnm = d["DEVICE_NAME"]

            phones, emails, org, centre = get_contact_info(devid)

            # get last reading
            cursor.execute("""
                SELECT READING_DATE, READING_TIME
                FROM device_reading_log
                WHERE DEVICE_ID=%s
                ORDER BY READING_DATE DESC, READING_TIME DESC
                LIMIT 1
            """, (devid,))
            last = cursor.fetchone()

            diff = None
            if last:
                rd = last["READING_DATE"]
                rt = parse_reading_time(last["READING_TIME"])
                if rd and rt:
                    lastdt = datetime.combine(rd, rt)
                    diff = (now - lastdt).total_seconds() / 60
                    if diff < 0:
                        diff = OFFLINE_THRESHOLD + 5
                else:
                    diff = OFFLINE_THRESHOLD + 5
            else:
                diff = OFFLINE_THRESHOLD + 5

            # online/offline
            is_online = diff <= OFFLINE_THRESHOLD

            # get existing alarm
            cursor.execute("""
                SELECT *
                FROM device_status_alarm_log
                WHERE DEVICE_ID=%s AND IS_ACTIVE=1
                ORDER BY DEVICE_STATUS_ALARM_ID DESC LIMIT 1
            """, (devid,))
            existing = cursor.fetchone()

            # ---------------- ONLINE ----------------
            if is_online:
                if existing:
                    msg = build_message(5, devnm)
                    sms_ok = False
                    email_ok = False

                    for ph in phones:
                        if send_sms(ph, msg):
                            sms_ok = True

                    if emails:
                        email_ok = send_email(f"{devnm} Status Update", msg, emails)

                    cursor.execute("""
                        UPDATE device_status_alarm_log
                        SET IS_ACTIVE=0,
                            UPDATED_ON_DATE=%s,
                            UPDATED_ON_TIME=%s
                        WHERE DEVICE_STATUS_ALARM_ID=%s
                    """, (now.date(), now.time(), existing["DEVICE_STATUS_ALARM_ID"]))
                    conn.commit()

                continue

            # ---------------- OFFLINE ----------------
            if not existing:
                log("➡ Creating new offline alarm (first notification is automatic).")

                sms_msg = build_message(3, devnm)
                for ph in phones:
                    send_sms(ph, sms_msg)

                email_sent = False
                if emails:
                    email_sent = send_email(f"{devnm} Status Update", sms_msg, emails)

                cursor.execute("""
                    INSERT INTO device_status_alarm_log
                    (DEVICE_ID, DEVICE_STATUS, IS_ACTIVE,
                     CREATED_ON_DATE, CREATED_ON_TIME,
                     SMS_DATE, SMS_TIME,
                     EMAIL_DATE, EMAIL_TIME)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """, (
                    devid, 1, 1,
                    now.date(), now.time(),
                    now.date(), now.time(),           # ALWAYS record first alert
                    now.date() if email_sent else None,
                    now.time() if email_sent else None
                ))
                conn.commit()
                continue

            # existing offline alarm → check repeat SMS logic
            sms_date = existing["SMS_DATE"]
            sms_time = existing["SMS_TIME"]

            if not sms_date or not sms_time:
                # Should NEVER happen now because INSERT logs first SMS time
                continue
            
            # ---- FIX: ensure sms_time is datetime.time ----
            if isinstance(sms_time, timedelta):
              total_seconds = int(sms_time.total_seconds())
              sms_time = dt_time(
              total_seconds // 3600,
              (total_seconds % 3600) // 60,
              total_seconds % 60
           )
            last_sms_dt = datetime.combine(sms_date, sms_time)

            # 6 hour rule
            if now >= last_sms_dt + timedelta(hours=SECOND_NOTIFICATION_HOURS):
                log("➡ 6+ hours passed → sending repeat SMS")
                sms_msg = build_message(3, devnm)

                sms_ok = False
                for ph in phones:
                    if send_sms(ph, sms_msg):
                        sms_ok = True

                if sms_ok:
                    cursor.execute("""
                        UPDATE device_status_alarm_log
                        SET SMS_DATE=%s, SMS_TIME=%s
                        WHERE DEVICE_STATUS_ALARM_ID=%s
                    """, (now.date(), now.time(), existing["DEVICE_STATUS_ALARM_ID"]))
                    conn.commit()
            else:
                log("➡ SMS sent recently (<6 hrs), not sending again.")

        log("✅ Processing complete.")

    except Exception as e:
        log(f"❌ Error: {e}")
        traceback.print_exc()

    finally:
        if cursor: 
            try: cursor.close()
            except: pass
        if conn and conn.is_connected():
            conn.close()


if __name__ == "__main__":
    check_device_online_status()


# import mysql.connector
# import time as t
# from datetime import datetime, time as dt_time, timedelta, date
# import requests
# import smtplib
# from email.mime.text import MIMEText
# import json, os

# # ================== CONFIG ==================
# db_config = {
#     "host": "switchback.proxy.rlwy.net",
#     "user": "root",
#     "port": 44750,
#     "password": "qYxlhEiaEvtiRvKaFyigDPtXSSCpddMv",
#     "database": "railway",
# }

# SMS_API_URL = "http://www.universalsmsadvertising.com/universalsmsapi.php"
# SMS_USER = "8960853914"
# SMS_PASS = "8960853914"
# SENDER_ID = "FRTLLP"

# SMTP_SERVER = "smtp.gmail.com"
# SMTP_PORT = 587
# EMAIL_USER = "testwebservice71@gmail.com"
# EMAIL_PASS = "akuu vulg ejlg ysbt"

# OFFLINE_THRESHOLD = 10         # minutes
# SECOND_NOTIFICATION_HOURS = 6  # wait 6 hours

# # ================== HELPERS ==================
# def build_message(ntf_typ, devnm):
#     messages = {
#         3: f"WARNING!! The {devnm} is offline. Please take necessary action - Regards Fertisense LLP",
#         5: f"INFO!! The device {devnm} is back online. No action is required - Regards Fertisense LLP",
#     }
#     return messages.get(ntf_typ, f"Alert for {devnm} - Regards Fertisense LLP")

# def send_sms(phone, message):
#     try:
#         params = {
#             "user_name": SMS_USER,
#             "user_password": SMS_PASS,
#             "mobile": phone,
#             "sender_id": SENDER_ID,
#             "type": "F",
#             "text": message
#         }
#         requests.get(SMS_API_URL, params=params, timeout=10)
#         print(f"✅ SMS sent: {phone}")
#         return True
#     except Exception as e:
#         print("❌ SMS failed:", e)
#         return False

# def send_email(subject, message, email_ids):
#     if not email_ids:
#         return False
#     try:
#         msg = MIMEText(message)
#         msg["Subject"] = subject
#         msg["From"] = EMAIL_USER
#         msg["To"] = ", ".join(email_ids)
#         server = smtplib.SMTP(SMTP_SERVER, SMTP_PORT)
#         server.starttls()
#         server.login(EMAIL_USER, EMAIL_PASS)
#         server.sendmail(EMAIL_USER, email_ids, msg.as_string())
#         server.quit()
#         print("✅ Email sent:", subject)
#         return True
#     except Exception as e:
#         print("❌ Email failed:", e)
#         return False


# # =============== DEVICE STATUS ALARM LOG TABLE ===============
# # DEVICE_STATUS_ALARM_ID (PK)
# # DEVICE_ID
# # DEVICE_STATUS (1 active, 0 scrap)
# # IS_ACTIVE (1 = offline alarm active, 0 = resolved)
# # CREATED_ON_DATE, CREATED_ON_TIME
# # UPDATED_ON_DATE, UPDATED_ON_TIME
# # SMS_DATE, SMS_TIME
# # EMAIL_DATE, EMAIL_TIME


# # ================== MAIN LOGIC ==================
# def check_device_online_status():
#     try:
#         print("🚀 Starting Script...")
#         conn = mysql.connector.connect(**db_config)
#         cursor = conn.cursor(dictionary=True)
#         now = datetime.now()

#         cursor.execute("SELECT DEVICE_ID, DEVICE_NAME FROM iot_api_masterdevice WHERE DEVICE_STATUS = 1")
#         devices = cursor.fetchall()
#         print(f"✅ Found {len(devices)} active devices")

#         for device in devices:
#             devid = str(device["DEVICE_ID"])
#             devnm = device["DEVICE_NAME"]

#             # Last reading
#             cursor.execute("""
#                 SELECT READING_DATE, READING_TIME 
#                 FROM device_reading_log 
#                 WHERE DEVICE_ID=%s 
#                 ORDER BY READING_DATE DESC, READING_TIME DESC LIMIT 1
#             """, (devid,))
#             last_read = cursor.fetchone()

#             diff_minutes = None
#             if last_read:
#                 reading_time = last_read["READING_TIME"]
#                 if isinstance(reading_time, timedelta):
#                     total_sec = reading_time.total_seconds()
#                     reading_time = dt_time(
#                         int(total_sec // 3600),
#                         int((total_sec % 3600) // 60),
#                         int(total_sec % 60)
#                     )
#                 last_update = datetime.combine(last_read["READING_DATE"], reading_time)
#                 diff_minutes = (now - last_update).total_seconds() / 60

#             current_state = 0 if (diff_minutes is None or diff_minutes > OFFLINE_THRESHOLD) else 1

#             # ---- GET LATEST OPEN ALARM ----
#             cursor.execute("""
#                 SELECT * FROM device_status_alarm_log
#                 WHERE DEVICE_ID=%s AND IS_ACTIVE=1
#                 ORDER BY DEVICE_STATUS_ALARM_ID DESC LIMIT 1
#             """, (devid,))
#             existing = cursor.fetchone()

#             # ==================================================================
#             #                          DEVICE ONLINE
#             # ==================================================================
#             if current_state == 1:
#                 print(f"✅ {devnm} is ONLINE")

#                 if existing:
#                     print("➡ Closing open offline alarm & sending ONLINE SMS/Email.")

#                     message = build_message(5, devnm)

#                     # For now use dummy numbers (you insert your logic here)
#                     phones = []
#                     emails = []

#                     sms_sent = any(send_sms(p, message) for p in phones)
#                     email_sent = send_email(f"{devnm} Status Update", message, emails)

#                     cursor.execute("""
#                         UPDATE device_status_alarm_log
#                         SET IS_ACTIVE=0,
#                             UPDATED_ON_DATE=%s,
#                             UPDATED_ON_TIME=%s,
#                             SMS_DATE=%s,
#                             SMS_TIME=%s,
#                             EMAIL_DATE=%s,
#                             EMAIL_TIME=%s
#                         WHERE DEVICE_STATUS_ALARM_ID=%s
#                     """, (
#                         now.date(), now.time(),
#                         now.date() if sms_sent else existing["SMS_DATE"],
#                         now.time() if sms_sent else existing["SMS_TIME"],
#                         now.date() if email_sent else existing["EMAIL_DATE"],
#                         now.time() if email_sent else existing["EMAIL_TIME"],
#                         existing["DEVICE_STATUS_ALARM_ID"]
#                     ))
#                     conn.commit()
#                 continue

#             # ==================================================================
#             #                          DEVICE OFFLINE
#             # ==================================================================
#             print(f"🚨 {devnm} is OFFLINE")

#             # -------- Case A: No active offline alarm → create new --------
#             if not existing:
#                 print("➡ Creating new offline alarm & sending SMS/Email.")

#                 message = build_message(3, devnm)
#                 phones = []
#                 emails = []

#                 sms_sent = any(send_sms(p, message) for p in phones)
#                 email_sent = send_email(f"{devnm} Status Update", message, emails)

#                 cursor.execute("""
#                     INSERT INTO device_status_alarm_log
#                     (DEVICE_ID, DEVICE_STATUS, IS_ACTIVE,
#                      CREATED_ON_DATE, CREATED_ON_TIME,
#                      SMS_DATE, SMS_TIME, EMAIL_DATE, EMAIL_TIME)
#                     VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
#                 """, (
#                     devid, 1, 1,
#                     now.date(), now.time(),
#                     now.date() if sms_sent else None,
#                     now.time() if sms_sent else None,
#                     now.date() if email_sent else None,
#                     now.time() if email_sent else None
#                 ))
#                 conn.commit()
#                 print("➕ New offline alarm stored.")
#                 continue

#             # -------- Case B: Offline alarm exists → check 6-hour rule --------
#             print("➡ Checking SMS timing for 6-hour rule...")

#             sms_last_dt = None
#             if existing["SMS_DATE"] and existing["SMS_TIME"]:
#                 sms_last_dt = datetime.combine(existing["SMS_DATE"], existing["SMS_TIME"])

#             # Send if never sent before
#             if not sms_last_dt:
#                 print("➡ First SMS not sent earlier. Sending now.")
#                 message = build_message(3, devnm)
#                 phones = []
#                 sms_sent = any(send_sms(p, message) for p in phones)

#                 cursor.execute("""
#                     UPDATE device_status_alarm_log
#                     SET SMS_DATE=%s, SMS_TIME=%s
#                     WHERE DEVICE_STATUS_ALARM_ID=%s
#                 """, (now.date(), now.time(), existing["DEVICE_STATUS_ALARM_ID"]))
#                 conn.commit()
#                 continue

#             # Check 6-hour gap
#             if now >= sms_last_dt + timedelta(hours=SECOND_NOTIFICATION_HOURS):
#                 print("➡ More than 6 hrs passed. Sending SMS again.")
#                 message = build_message(3, devnm)
#                 phones = []
#                 sms_sent = any(send_sms(p, message) for p in phones)

#                 cursor.execute("""
#                     UPDATE device_status_alarm_log
#                     SET SMS_DATE=%s, SMS_TIME=%s
#                     WHERE DEVICE_STATUS_ALARM_ID=%s
#                 """, (now.date(), now.time(), existing["DEVICE_STATUS_ALARM_ID"]))
#                 conn.commit()
#             else:
#                 print("➡ SMS already sent < 6 hrs. No new SMS.")

#         cursor.close()
#         conn.close()
#         print("✅ Script Completed.")

#     except Exception as e:
#         print("❌ Error in check_device_online_status:", e)


# if __name__ == "__main__":
#     check_device_online_status()


# import mysql.connector
# import time as t
# from datetime import datetime, time as dt_time, timedelta , date
# import requests
# import smtplib
# from email.mime.text import MIMEText
# import json, os

# # ================== CONFIG ==================
# db_config = {
#     "host": "switchback.proxy.rlwy.net",
#     "user": "root",
#     "port": 44750,
#     "password": "qYxlhEiaEvtiRvKaFyigDPtXSSCpddMv",
#     "database": "railway",
# }

# SMS_API_URL = "http://www.universalsmsadvertising.com/universalsmsapi.php"
# SMS_USER = "8960853914"
# SMS_PASS = "8960853914"
# SENDER_ID = "FRTLLP"

# SMTP_SERVER = "smtp.gmail.com"
# SMTP_PORT = 587
# EMAIL_USER = "testwebservice71@gmail.com"
# EMAIL_PASS = "akuu vulg ejlg ysbt"

# OFFLINE_THRESHOLD = 5          # minutes
# OFFLINE_VERIFY_MINUTES = 3     # wait before confirming offline
# SECOND_NOTIFICATION_HOURS = 6  # wait 6 hours before re-alert

# STATE_FILE = "notification_state.json"

# # ================== STATE FILE HANDLERS ==================
# def load_state():
#     """Load notification state from JSON."""
#     if os.path.exists(STATE_FILE):
#         try:
#             with open(STATE_FILE, "r") as f:
#                 return json.load(f)
#         except json.JSONDecodeError:
#             print("⚠️ State file corrupted, resetting.")
#             return {}
#     return {}

# def save_state(state):
#     """Save state back to JSON."""
#     with open(STATE_FILE, "w") as f:
#         json.dump(state, f, indent=2)

# # ================== HELPERS ==================
# def build_message(ntf_typ, devnm):
#     messages = {
#         3: f"WARNING!! The {devnm} is offline. Please take necessary action - Regards Fertisense LLP",
#         5: f"INFO!! The device {devnm} is back online. No action is required - Regards Fertisense LLP",
#     }
#     return messages.get(ntf_typ, f"Alert for {devnm} - Regards Fertisense LLP")

# def send_sms(phone, message):
#     try:
#         params = {
#             "user_name": SMS_USER,
#             "user_password": SMS_PASS,
#             "mobile": phone,
#             "sender_id": SENDER_ID,
#             "type": "F",
#             "text": message
#         }
#         requests.get(SMS_API_URL, params=params, timeout=10)
#         print(f"✅ SMS sent: {phone}")
#         return True
#     except Exception as e:
#         print("❌ SMS failed:", e)
#         return False

# def send_email(subject, message, email_ids):
#     if not email_ids:
#         return False
#     try:
#         msg = MIMEText(message)
#         msg["Subject"] = subject
#         msg["From"] = EMAIL_USER
#         msg["To"] = ", ".join(email_ids)
#         server = smtplib.SMTP(SMTP_SERVER, SMTP_PORT)
#         server.starttls()
#         server.login(EMAIL_USER, EMAIL_PASS)
#         server.sendmail(EMAIL_USER, email_ids, msg.as_string())
#         server.quit()
#         print("✅ Email sent:", subject)
#         return True
#     except Exception as e:
#         print("❌ Email failed:", e)
#         return False



# def get_contact_info(device_id):
#     """Fetch contacts only if device has valid subscription_id=8 and Subcription_End_date >= today."""
#     try:
#         conn = mysql.connector.connect(**db_config)
#         cursor = conn.cursor(dictionary=True)

#         today = date.today()

#         # Check subscription with join to get package info
#         cursor.execute("""
#             SELECT sh.*, msi.Package_Name
#             FROM Subcription_History sh
#             JOIN Master_Subscription_Info msi
#               ON sh.Subscription_ID = msi.Subscription_ID
#             WHERE sh.Device_ID=%s
#               AND sh.Subscription_ID=8
#               AND sh.Subcription_End_date >= %s
#         """, (device_id, today))
#         subscription = cursor.fetchone()

#         # Debug
#         print(f"DEBUG: subscription for device {device_id}:", subscription)

#         if not subscription:
#             return [], [], 1, 1  # no valid subscription → skip alerts

#         # Device info
#         cursor.execute("SELECT ORGANIZATION_ID, CENTRE_ID FROM master_device WHERE DEVICE_ID=%s", (device_id,))
#         device = cursor.fetchone()
#         if not device:
#             return [], [], 1, 1

#         org_id = device["ORGANIZATION_ID"] or 1
#         centre_id = device["CENTRE_ID"] or 1

#         # Users linked to org+centre
#         cursor.execute("""
#             SELECT USER_ID_id FROM userorganizationcentrelink 
#             WHERE ORGANIZATION_ID_id=%s AND CENTRE_ID_id=%s
#         """, (org_id, centre_id))
#         user_ids = [u["USER_ID_id"] for u in cursor.fetchall()]
#         if not user_ids:
#             return [], [], org_id, centre_id

#         format_strings = ','.join(['%s']*len(user_ids))
#         cursor.execute(f"""
#             SELECT PHONE, EMAIL, SEND_SMS, SEND_EMAIL
#             FROM master_user 
#             WHERE USER_ID IN ({format_strings})
#               AND (SEND_SMS=1 OR SEND_EMAIL=1)
#         """, tuple(user_ids))
#         users = cursor.fetchall()

#         phones = [u["PHONE"] for u in users if u["SEND_SMS"] == 1]
#         emails = [u["EMAIL"] for u in users if u["SEND_EMAIL"] == 1]
#         return phones, emails, org_id, centre_id

#     except Exception as e:
#         print("❌ Error getting contacts:", e)
#         return [], [], 1, 1
#     finally:
#         if 'cursor' in locals():
#             cursor.close()
#         if 'conn' in locals() and conn.is_connected():
#             conn.close()

# # ================== MAIN LOGIC ==================
# def check_device_online_status():
#     try:
#         print("🚀 Starting Script...")
#         conn = mysql.connector.connect(**db_config)
#         cursor = conn.cursor(dictionary=True)
#         now = datetime.now()

#         cursor.execute("SELECT DEVICE_ID, DEVICE_NAME FROM master_device WHERE DEVICE_STATUS = 1")
#         devices = cursor.fetchall()
#         print(f"✅ Found {len(devices)} active devices")

#         # Load previous notification data
#         state = load_state()
#         print(f"🧾 Loaded {len(state)} records from JSON")

#         for device in devices:
#             devid = str(device["DEVICE_ID"])  # ensure string keys
#             devnm = device["DEVICE_NAME"]

#                 # --------- CHECK SUBSCRIPTION FIRST ----------
#             phones, emails, org_id, centre_id = get_contact_info(devid)
#             if not phones and not emails:
#                 print(f"⏹ {devnm} skipped (no valid subscription)")
#                 continue  # skip this device entirely

#             # Get last reading
#             cursor.execute("""
#                 SELECT READING_DATE, READING_TIME 
#                 FROM device_reading_log 
#                 WHERE DEVICE_ID=%s 
#                 ORDER BY READING_DATE DESC, READING_TIME DESC LIMIT 1
#             """, (devid,))
#             last_read = cursor.fetchone()
#             cursor.fetchall()

#             diff_minutes = None
#             if last_read:
#                 reading_time = last_read["READING_TIME"]
#                 if isinstance(reading_time, timedelta):
#                     total_sec = reading_time.total_seconds()
#                     reading_time = dt_time(int(total_sec // 3600), int((total_sec % 3600) // 60), int(total_sec % 60))
#                 last_update = datetime.combine(last_read["READING_DATE"], reading_time)
#                 diff_minutes = (now - last_update).total_seconds() / 60

#             current_state = 0 if (diff_minutes is None or diff_minutes > OFFLINE_THRESHOLD) else 1

#             # Verify offline devices again
#             if current_state == 0:
#                 print(f"⚠️ {devnm} appears OFFLINE, verifying...")
#                 verify_until = datetime.now() + timedelta(minutes=OFFLINE_VERIFY_MINUTES)
#                 while datetime.now() < verify_until:
#                     cursor.execute("""
#                         SELECT READING_DATE, READING_TIME 
#                         FROM device_reading_log 
#                         WHERE DEVICE_ID=%s 
#                         ORDER BY READING_DATE DESC, READING_TIME DESC LIMIT 1
#                     """, (devid,))
#                     last_check = cursor.fetchone()
#                     cursor.fetchall()
#                     if last_check:
#                         reading_time = last_check["READING_TIME"]
#                         if isinstance(reading_time, timedelta):
#                             total_sec = reading_time.total_seconds()
#                             reading_time = dt_time(int(total_sec // 3600), int((total_sec % 3600) // 60), int(total_sec % 60))
#                         last_update_check = datetime.combine(last_check["READING_DATE"], reading_time)
#                         diff_check = (datetime.now() - last_update_check).total_seconds() / 60
#                         if diff_check <= OFFLINE_THRESHOLD:
#                             print(f"✅ {devnm} came back online within {OFFLINE_VERIFY_MINUTES} minutes.")
#                             current_state = 1
#                             break
#                     t.sleep(30)

#             # ---------------- Notification Logic ----------------
#             now_time = datetime.now()
#             record = state.get(devid, {})
#             last_state = record.get("last_state")
#             last_notif_time = record.get("last_notif_time")

#             can_notify = False
#             reason = ""

#             if last_state != current_state:
#                 can_notify = True
#                 reason = "State changed"
#             elif last_notif_time:
#                 last_notif_dt = datetime.fromisoformat(last_notif_time)
#                 if (now_time - last_notif_dt) >= timedelta(hours=SECOND_NOTIFICATION_HOURS):
#                     can_notify = True
#                     reason = "6-hour reminder"

#             if can_notify:
#                 phones, emails, org_id, centre_id = get_contact_info(devid)
#                 sms_sent = False
#                 email_sent = False

#                 if current_state == 0:
#                     print(f"🚨 {devnm} confirmed OFFLINE! Sending alerts. ({reason})")
#                     message = build_message(3, devnm)
#                 else:
#                     print(f"✅ {devnm} is ONLINE! Sending info alert. ({reason})")
#                     message = build_message(5, devnm)

#                 for phone in phones:
#                     if send_sms(phone, message):
#                         sms_sent = True
#                 email_sent = send_email(f"{devnm} Status Update", message, emails)

#                 # DB log
#                 cursor.execute("SELECT id FROM iot_api_devicealarmlog WHERE DEVICE_ID=%s AND ALARM_DATE=%s",
#                                (devid, now.date()))
#                 existing = cursor.fetchone()
#                 cursor.fetchall()

#                 if existing:
#                     cursor.execute("""
#                         UPDATE iot_api_devicealarmlog
#                         SET DEVICE_STATUS=%s,
#                             DEVICE_STATUS_DATE=%s,
#                             DEVICE_STATUS_TIME=%s,
#                             DEVICE_STATUS_SMS_DATE=%s,
#                             DEVICE_STATUS_SMS_TIME=%s,
#                             DEVICE_STATUS_EMAIL_DATE=%s,
#                             DEVICE_STATUS_EMAIL_TIME=%s,
#                             ORGANIZATION_ID=%s,
#                             CENTRE_ID=%s
#                         WHERE id=%s
#                     """, (
#                         current_state,
#                         now.date(),
#                         now.time(),
#                         now.date() if sms_sent else None,
#                         now.time() if sms_sent else None,
#                         now.date() if email_sent else None,
#                         now.time() if email_sent else None,
#                         org_id,
#                         centre_id,
#                         existing['id']
#                     ))
#                     print(f"📝 Updated alarm log for {devnm}")
#                 else:
#                     cursor.execute("""
#                         INSERT INTO iot_api_devicealarmlog
#                         (DEVICE_ID, SENSOR_ID, PARAMETER_ID, ALARM_DATE, ALARM_TIME,
#                          DEVICE_STATUS, DEVICE_STATUS_DATE, DEVICE_STATUS_TIME,
#                          DEVICE_STATUS_SMS_DATE, DEVICE_STATUS_SMS_TIME,
#                          DEVICE_STATUS_EMAIL_DATE, DEVICE_STATUS_EMAIL_TIME,
#                          ORGANIZATION_ID, CENTRE_ID)
#                          VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
#                     """, (
#                         devid, 0, 0,
#                         now.date(), now.time(),
#                         current_state, now.date(), now.time(),
#                         now.date() if sms_sent else None, now.time() if sms_sent else None,
#                         now.date() if email_sent else None, now.time() if email_sent else None,
#                         org_id, centre_id
#                     ))
#                     print(f"➕ Inserted new alarm log for {devnm}")

#                 conn.commit()

#                 # ✅ Update state file
#                 state[devid] = {
#                     "last_state": current_state,
#                     "last_notif_time": now_time.isoformat()
#                 }
#                 save_state(state)
#                 print(f"💾 State updated for {devnm}")

#             else:
#                 print(f"⏳ {devnm} skipped (same state, no cooldown reached).")

#         cursor.close()
#         conn.close()
#         print("✅ Done... Ending Script.")
#     except Exception as e:
#         print("❌ Error in check_device_online_status:", e)

# # ================== RUN ==================
# if __name__ == "__main__":
#     check_device_online_status()
