import smtplib
from email.mime.text import MIMEText
from config import config_dev

def send_email(Subject: str, body: str):
    sender = config_dev["email_sender"]
    receiver = config_dev["email_receiver"]
    msg = MIMEText(body)
    msg["Subject"] = Subject
    msg["from"] = sender
    msg["To"] = receiver

    with smtplib.SMTP("smtp.gmail.com",587) as server:
        server.starttls()
        server.login(sender, "Surya@0686")
        server.sendemail(sender, receiver, msg.as_string())