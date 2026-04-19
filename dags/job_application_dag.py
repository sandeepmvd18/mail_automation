"""
Job Application Email DAG
--------------------------
This DAG sends job application emails with resume to a list of email IDs.

HOW TO USE:
1. Create a .env file in the project root with your credentials
2. Run: docker-compose up -d
3. Place your resume PDF at resume/resume.pdf
4. Trigger the DAG manually from Airflow UI
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import smtplib
import ssl
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
import os
import logging

# ============================================================
# RECIPIENT LIST - Tuple format: (email, hiring_manager_name)
# Use None for name if unknown → falls back to "Hiring Manager"
# ============================================================
RECIPIENT_EMAILS = [
    ("sandeepmv080@gmail.com", "Sandeep"),         # ← replace with real name
    # ("hr@company2.com", "Sarah"),
    # ("recruiter@company3.com", None),          # ← None = "Hiring Manager"
]

# Resume path inside the container (mapped via docker volume)
RESUME_PATH = "/opt/airflow/resume/resume.pdf"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def get_config():
    """Read environment variables at runtime (inside task), not at import time."""
    return {
        "sender_email":    os.environ.get("SENDER_EMAIL", ""),
        "sender_password": os.environ.get("SENDER_PASSWORD", ""),
        "your_name":       os.environ.get("YOUR_NAME", "Sasidhar Jonna"),
        "your_phone":      os.environ.get("YOUR_PHONE", ""),
        "your_linkedin":   os.environ.get("YOUR_LINKEDIN", "https://www.linkedin.com/in/sasidharjonna"),
        "your_portfolio":  os.environ.get("YOUR_PORTFOLIO", "https://portfolio-e4h8.onrender.com/"),
    }


def get_email_subject(your_name: str) -> str:
    return f"Reaching Out Regarding Opportunities – {your_name}"


def get_email_body(cfg: dict, manager_name: str = None) -> str:
    salutation = f"Hi {manager_name}," if manager_name else "Dear Hiring Manager,"
    return f"""
    <html>
    <body style="font-family: Arial, sans-serif; font-size: 14px; color: #333333; line-height: 1.8; max-width: 650px;">

        <p>{salutation}</p>

        <p>I hope you're doing well.</p>

        <p>My name is <strong>Sasidhar</strong>, and I'm currently working as an
        <strong>Associate Software Engineer</strong>. I'm exploring new
        <strong>Software Engineer opportunities</strong> where I can contribute to building
        scalable systems and continue growing as a developer.</p>

        <p>I have strong fundamentals in <strong>Data Structures &amp; Algorithms</strong> and
        <strong>System Design</strong>, along with hands-on experience working with
        <strong>Python, REST APIs, Apache Airflow, React, and Node.js</strong>. In my current
        role, I work on enterprise integrations and data pipelines handling large-scale
        production workloads.</p>

        <p>I'd really appreciate it if you could take a look at my profile. I've attached my
        resume for your reference.</p>

        <p><strong>Portfolio:</strong>
        <a href="{cfg['your_portfolio']}">{cfg['your_portfolio']}</a></p>

        <p>If there are any relevant openings in your team, I would be happy to connect and
        discuss further.</p>

        <p>Thank you for your time.</p>

        <p>
            Best regards,<br>
            <strong>{cfg['your_name']}</strong><br>
            Bengaluru, India<br>
            <a href="tel:{cfg['your_phone']}">{cfg['your_phone']}</a><br>
            <a href="mailto:{cfg['sender_email']}">{cfg['sender_email']}</a><br>
            <a href="{cfg['your_linkedin']}">LinkedIn</a> &nbsp;|&nbsp;
            <a href="{cfg['your_portfolio']}">Portfolio</a>
        </p>

    </body>
    </html>
    """


def validate_configuration(**context):
    """Task 1: Validates that all env vars are set."""
    logging.info("=== Validating Configuration ===")

    cfg = get_config()
    errors = []

    if not cfg["sender_email"]:
        errors.append("❌ SENDER_EMAIL is not set in environment / .env file")
    if not cfg["sender_password"]:
        errors.append("❌ SENDER_PASSWORD is not set in environment / .env file")
    if not cfg["your_name"]:
        errors.append("❌ YOUR_NAME is not set in environment / .env file")
    if not RECIPIENT_EMAILS:
        errors.append("❌ RECIPIENT_EMAILS list is empty in the DAG file")

    if not os.path.exists(RESUME_PATH):
        logging.warning(f"⚠️  Resume not found at {RESUME_PATH} — emails will send WITHOUT attachment")
    else:
        logging.info(f"✅ Resume found: {RESUME_PATH}")

    if errors:
        for err in errors:
            logging.error(err)
        raise ValueError("Configuration validation failed. Fix errors above and re-trigger.")

    logging.info(f"✅ SENDER_EMAIL    : {cfg['sender_email']}")
    logging.info(f"✅ YOUR_NAME       : {cfg['your_name']}")
    logging.info(f"✅ Recipients      : {len(RECIPIENT_EMAILS)}")
    context["ti"].xcom_push(key="total_recipients", value=len(RECIPIENT_EMAILS))
    return "Validation passed"


def send_job_application_emails(**context):
    """Task 2: Sends job application emails to all recipients."""
    logging.info("=== Starting Email Sending Process ===")

    cfg = get_config()

    success_count = 0
    failed_emails = []

    ssl_context = ssl.create_default_context()

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=ssl_context) as server:
            logging.info("Connecting to Gmail SMTP server...")
            server.login(cfg["sender_email"], cfg["sender_password"])
            logging.info("✅ Gmail login successful!")

            for idx, entry in enumerate(RECIPIENT_EMAILS, 1):
                # Support both (email, name) tuples and plain email strings
                if isinstance(entry, tuple):
                    recipient_email, manager_name = entry[0], entry[1]
                else:
                    recipient_email, manager_name = entry, None

                logging.info(f"Sending {idx}/{len(RECIPIENT_EMAILS)} → {recipient_email} (Name: {manager_name or 'N/A'})")

                try:
                    msg = MIMEMultipart("alternative")
                    msg["Subject"] = get_email_subject(cfg["your_name"])
                    msg["From"]    = f"{cfg['your_name']} <{cfg['sender_email']}>"
                    msg["To"]      = recipient_email

                    msg.attach(MIMEText(get_email_body(cfg, manager_name), "html"))

                    # Attach resume if present
                    if os.path.exists(RESUME_PATH):
                        with open(RESUME_PATH, "rb") as f:
                            part = MIMEBase("application", "octet-stream")
                            part.set_payload(f.read())
                        encoders.encode_base64(part)
                        filename = cfg["your_name"].replace(" ", "_") + "_Resume.pdf"
                        part.add_header("Content-Disposition", f"attachment; filename={filename}")
                        msg.attach(part)
                        logging.info("  📎 Resume attached")

                    server.sendmail(cfg["sender_email"], recipient_email, msg.as_string())
                    success_count += 1
                    logging.info(f"  ✅ Sent to {recipient_email}")

                except Exception as e:
                    logging.error(f"  ❌ Failed for {recipient_email}: {e}")
                    failed_emails.append({"email": recipient_email, "error": str(e)})

    except smtplib.SMTPAuthenticationError:
        raise Exception(
            "Gmail authentication failed!\n"
            "→ Make sure you are using a Gmail App Password, NOT your regular password.\n"
            "→ Generate one at: https://myaccount.google.com/apppasswords"
        )
    except Exception as e:
        raise Exception(f"SMTP error: {e}")

    logging.info(f"✅ Success: {success_count}/{len(RECIPIENT_EMAILS)}")
    if failed_emails:
        for f in failed_emails:
            logging.warning(f"  ❌ {f['email']}: {f['error']}")

    context["ti"].xcom_push(key="success_count", value=success_count)
    context["ti"].xcom_push(key="failed_emails", value=failed_emails)
    return f"Sent {success_count}/{len(RECIPIENT_EMAILS)} emails"


def generate_report(**context):
    """Task 3: Logs a summary report."""
    cfg = get_config()
    ti = context["ti"]
    success_count = ti.xcom_pull(task_ids="send_emails", key="success_count") or 0
    failed_emails  = ti.xcom_pull(task_ids="send_emails", key="failed_emails") or []
    total = len(RECIPIENT_EMAILS)

    logging.info("=" * 50)
    logging.info("   JOB APPLICATION EMAIL CAMPAIGN REPORT")
    logging.info("=" * 50)
    logging.info(f"  Sender      : {cfg['your_name']} ({cfg['sender_email']})")
    logging.info(f"  Run Date    : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logging.info(f"  Total       : {total}")
    logging.info(f"  ✅ Sent     : {success_count}")
    logging.info(f"  ❌ Failed   : {len(failed_emails)}")
    logging.info(f"  Success Rate: {round(success_count/total*100,1) if total else 0}%")
    if failed_emails:
        logging.info("  Failed:")
        for f in failed_emails:
            logging.info(f"    - {f['email']}: {f['error']}")
    logging.info("=" * 50)
    logging.info("  Good luck with your job search! 🚀")
    logging.info("=" * 50)
    return "Report done"


# ============================================================
# DAG DEFINITION
# ============================================================
with DAG(
    dag_id="job_application_email_dag",
    description="Send job application emails with resume to recruiters",
    default_args=default_args,
    schedule_interval=None,   # Manual trigger only
    start_date=days_ago(1),
    catchup=False,
    tags=["job-search", "email", "manual"],
) as dag:

    t1 = PythonOperator(task_id="validate_config",  python_callable=validate_configuration,       provide_context=True)
    t2 = PythonOperator(task_id="send_emails",       python_callable=send_job_application_emails,  provide_context=True)
    t3 = PythonOperator(task_id="generate_report",   python_callable=generate_report,              provide_context=True)

    t1 >> t2 >> t3