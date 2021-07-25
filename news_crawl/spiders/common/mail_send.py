import os
import smtplib
from email import message
from scrapy.spiders import Spider


def mail_send(spider: Spider, title: str, msg: str, kwargs_save: dict) -> None:
    '''
    メール送信。件名(title)と本文(msg)を引数で渡す。
    環境変数（EMAIL_FROM、EMAIL_TO、EMAIL_PASS）があることを前提とします。
    '''
    # (参考情報)
    # https://qiita.com/aj2727/items/81e5d67cbcbf7396e392
    # Pythonでメールを送信（Outlook）

    # エラー通知＝Offの指定がある場合、何もせず終了する。

    if 'error_notice' in kwargs_save:
        spider.logger.info('=== メール通知 Off')
        return

    # 接続設定情報
    smtp_host = 'smtp.live.com'
    smtp_port = 587
    from_email = os.environ['EMAIL_FROM']
    to_email = os.environ['EMAIL_TO']
    username = from_email
    password = os.environ['EMAIL_PASS']
    timeout_limit: float = 60  # 簡単にタイムアウトされないように、60秒を指定してみた。

    # メール作成
    mail = message.EmailMessage()
    mail.set_content(msg)  # 本文
    mail['Subject'] = title  # タイトル
    mail['From'] = from_email  # 送信元
    mail['To'] = to_email  # 送信先

    '''
    【メール送信】
    送信先ホスト、ポートを指定
    ehlo()でsmtpサーバーと今からやりとりしますよと呼んであげます。
    セキュアにするためにserver.starttls()と記述した後にもう一度server.ehlo()とします。
    次にホットメールでログインするための情報が聞かれるので、usernameとpasswordを記述します。
    ログインできたらsend_message()を使用して()の中に作成したメッセージ内容を入れます。
    一番最後にもうサーバーとやりとりしないので終了させるためにserver.quit()とします
    '''
    server = smtplib.SMTP(smtp_host, smtp_port, timeout=timeout_limit)
    server.ehlo()
    server.starttls()
    server.ehlo()
    server.login(username, password)
    server.send_message(mail)
    server.quit()

    spider.logger.info('=== メール通知完了')
