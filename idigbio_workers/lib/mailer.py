from __future__ import division, absolute_import, print_function
import os.path
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email.utils import COMMASPACE, formatdate
from email import encoders

from idb.helpers.logging import idblogger

logger = idblogger.getChild("mailer")

def send_mail(send_from, send_to, subject, text, files=[]):
    smtp = smtplib.SMTP("smtp.ufl.edu")
    assert type(send_to) == list
    assert type(files) == list

    msg = MIMEMultipart()
    msg['From'] = send_from
    msg['To'] = COMMASPACE.join(send_to)
    msg['Date'] = formatdate(localtime=True)
    msg['Subject'] = subject

    msg.attach(MIMEText(text,'plain'))

    for f in files:
        part = MIMEBase('application', "octet-stream")
        part.set_payload(open(f,"rb").read())
        Encoders.encode_base64(part)
        part.add_header('Content-Disposition', 'attachment; filename="%s"' % os.path.basename(f))
        msg.attach(part)

    try:
        smtp.sendmail(send_from, send_to, msg.as_string())
    except Exception:
        logger.exception("Failed sending email to %s", send_to)
    smtp.close()
