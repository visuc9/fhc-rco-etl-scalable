import smtplib
from email.message import EmailMessage
import logging


def send_email(subject: str, body: str):
    msg = EmailMessage()
    # To be configured
    port = 587
    smtp_server = ''
    msg['From'] = ''
    msg['To'] = ''
    msg['Subject'] = subject
    msg.set_content(body)
    password = ''

    try:
        with smtplib.SMTP(smtp_server, port=port) as server:
            server.starttls()
            server.login(msg['From'], password)
            server.send_message(msg)
    except:
        logging.exception('Could not send email.')