# 📧 Job Application Email DAG — Apache Airflow

An Airflow DAG that automatically sends personalized job application emails
with your resume attached to a list of recruiter/company email addresses.

---

## 📁 Project Structure

```
airflow_job_email_project/
├── dags/
│   └── job_application_dag.py   ← Main DAG file (edit this)
├── resume/
│   └── resume.pdf               ← Place YOUR resume here
├── docker-compose.yml           ← Start Airflow with Docker
├── requirements.txt
└── README.md
```

---

## ⚙️ Setup — Step by Step

### Step 1: Add Your Resume
Copy your resume PDF file into the `resume/` folder and name it `resume.pdf`:
```
resume/resume.pdf
```

---

### Step 2: Configure the DAG
Open `dags/job_application_dag.py` and update the **CONFIGURATION** section near the top:

```python
# LIST OF PEOPLE YOU WANT TO EMAIL
RECIPIENT_EMAILS = [
    "hr@company1.com",
    "recruiter@company2.com",
    "hiring@company3.com",
    # Add as many as you want...
]

# YOUR GMAIL DETAILS
SENDER_EMAIL    = "yourname@gmail.com"
SENDER_PASSWORD = "xxxx xxxx xxxx xxxx"   # Gmail App Password (see below)

# YOUR PERSONAL INFO (shown in email signature)
YOUR_NAME     = "Raj Kumar"
YOUR_PHONE    = "+91-9876543210"
YOUR_LINKEDIN = "https://linkedin.com/in/rajkumar"
```

---

### Step 3: Get a Gmail App Password
You cannot use your normal Gmail password. You need an **App Password**:

1. Go to your Google Account → https://myaccount.google.com/
2. Click **Security** → Enable **2-Step Verification** (if not already)
3. Go to **App Passwords** → https://myaccount.google.com/apppasswords
4. Select app: "Mail" → Select device: "Other (custom)" → name it "Airflow"
5. Copy the 16-character password (e.g., `abcd efgh ijkl mnop`)
6. Paste it into `SENDER_PASSWORD` in the DAG file

---

### Step 4: Start Airflow

**Option A — Using Docker (Recommended, easiest):**
```bash
# In the project folder
docker-compose up -d

# Wait ~30 seconds, then open browser
# URL: http://localhost:8080
# Username: admin
# Password: admin
```

**Option B — Using pip:**
```bash
pip install apache-airflow==2.8.0
export AIRFLOW_HOME=$(pwd)/airflow_home
airflow db init
airflow users create --username admin --password admin --role Admin \
  --firstname Admin --lastname User --email admin@example.com
airflow webserver -p 8080 &
airflow scheduler &
```

---

### Step 5: Trigger the DAG

1. Open Airflow UI at **http://localhost:8080**
2. Login with `admin` / `admin`
3. Find DAG: **`job_application_email_dag`**
4. Click the **▶ Trigger** button (top right)
5. Confirm and watch it run!

---

## 🔄 DAG Flow

```
validate_config  →  send_emails  →  generate_report
      ↓                  ↓                ↓
  Checks all        Loops through    Logs success/
  settings are      each email and   failure summary
  configured        sends with
                    resume attached
```

---

## 📧 What the Email Looks Like

**Subject:** `Job Application - Raj Kumar | Seeking New Opportunities`

**Body:**
> Dear Hiring Manager,
>
> I hope this email finds you well. My name is **Raj Kumar**, and I am actively
> looking for new job opportunities...
>
> [Professional email body with your contact info]

**Attachment:** `Raj_Kumar_Resume.pdf`

---

## 🛠️ Customizing the Email

To change the email body, edit the `get_email_body()` function in the DAG file.
It uses HTML, so you can add styling, change wording, etc.

---

## ❓ Troubleshooting

| Problem | Solution |
|---------|----------|
| `SMTPAuthenticationError` | Use App Password, not regular Gmail password |
| Resume not attached | Place `resume.pdf` in the `resume/` folder |
| DAG not showing in UI | Check DAG file has no Python syntax errors |
| Emails going to spam | Ask recipients to mark as "Not Spam" |

---

## 📝 Notes

- The DAG is set to **manual trigger only** (`schedule_interval=None`)
- Each email is sent individually (not bulk BCC) for better deliverability
- All send results are logged in Airflow task logs
- Failed emails are listed in the `generate_report` task log
