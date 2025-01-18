import psycopg2
from psycopg2 import sql
import smtplib
from email.mime.text import MIMEText
from datetime import datetime, timedelta

# Database Configuration
DB_CONFIG = {
    "host": "192.168.220.17",
    "database": "ngucc",
    "user": "postgres",
    "password": "Avis!123"
}

# Email Configuration
SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587
EMAIL_USER = "jitesh@avissupport.com"
EMAIL_PASS = "wlfhjfbpczhukutg"
ALERT_RECIPIENTS = ["rohit.shrivastava@cogenteservices.com", "ankush.sharma@cogenteservices.in", "aditya@avissupport.com"]

# List of table names to check
TABLES = [
    "broadnew_2_hopper", "default_2_hopper", "newcrmstarttamil_2_hopper",
    "newtelugunew_2_hopper", "startmalaya_2_hopper", "notkannada_2_hopper",
    "newcrmstartmalya_2_hopper", "newcrmstartkannad_2_hopper",
    "newcrmstart_2_hopper", "nottelugu_2_hopper", "notmalayalam_2_hopper",
    "nottamil_2_hopper"
]

# Stuck record duration (10 minutes)
STUCK_DURATION = timedelta(minutes=10)

def send_email_alert(table, stuck_records):
    subject = f"Alert: Stuck Records in {table}"
    body = f"The following records in table '{table}' were stuck for more than 10 minutes and have been deleted:\n\n{stuck_records}"
    
    msg = MIMEText(body)
    msg["Subject"] = subject
    msg["From"] = EMAIL_USER
     msg["To"] = ", ".join(ALERT_RECIPIENTS)

    try:
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls()
            server.login(EMAIL_USER, EMAIL_PASS)
            server.sendmail(EMAIL_USER, ALERT_RECIPIENT, msg.as_string())
        print(f"Email alert sent for table {table}.")
    except Exception as e:
        print(f"Failed to send email alert: {e}")

def check_and_delete_stuck_records():
    try:
        connection = psycopg2.connect(**DB_CONFIG)
        cursor = connection.cursor()

        now = datetime.now()
        stuck_time_threshold = now - STUCK_DURATION

        for table in TABLES:
            # Select stuck records
            query = sql.SQL("""
                SELECT processupdatedon 
                FROM {} 
                WHERE processupdatedon <= %s
            """).format(sql.Identifier(table))
            cursor.execute(query, (stuck_time_threshold,))
            stuck_records = cursor.fetchall()

            if stuck_records:
                # Delete stuck records
                delete_query = sql.SQL("""
                    DELETE FROM {} 
                    WHERE processupdatedon <= %s
                """).format(sql.Identifier(table))
                cursor.execute(delete_query, (stuck_time_threshold,))
                connection.commit()

                # Send alert email
                send_email_alert(table, stuck_records)

    except Exception as e:
        print(f"Error: {e}")
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()

if __name__ == "__main__":
    check_and_delete_stuck_records()
