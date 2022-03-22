import os
import sys
import smtplib
import logging
from logging import Logger
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication

logger: Logger = logging.getLogger('prefect.mail_send_attach')


def mail_attach_send(title: str, msg: str, filepath: str) -> None:
    '''添付ファイル付きメールの送信'''
    smtp_host = 'smtp.office365.com'
    smtp_port = 587
    username = os.environ['EMAIL_FROM']
    password = os.environ['EMAIL_PASS']
    to_email = os.environ['EMAIL_TO']
    from_email = os.environ['EMAIL_FROM']
    timeout_limit: float = 60  # 簡単にタイムアウトされないように、60秒を指定してみた。

    mail = MIMEMultipart()
    mail["Subject"] = title
    mail["From"] = from_email
    mail["To"] = to_email
    mail.attach(MIMEText(msg, "html"))

    with open(filepath, "rb") as f:
        mb = MIMEApplication(f.read())

    mb.add_header("Content-Disposition", "attachment", filename=filepath)
    mail.attach(mb)

    server = smtplib.SMTP(smtp_host, smtp_port, timeout=timeout_limit)
    server.ehlo()
    server.starttls()
    server.ehlo()
    server.login(username, password)
    server.sendmail(from_email, to_email, mail.as_string())
    server.quit()

    logger.info('=== メール通知完了 title = ' + title)


if __name__ == '__main__':
    mail_attach_send(
        title='test',
        msg='手動で実行',
        filepath="/home/mikuras/004_atelier/002_BrownieHideaway/test.xlsx"
    )
