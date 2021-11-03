import os
from scrapy.exceptions import CloseSpider


def environ_check() -> None:
    '''
    当プロジェクトで必要とする環境変数の有無をチェックする。
    '''
    # 参考情報
    # https://qiita.com/aj2727/items/81e5d67cbcbf7396e392
    # Pythonでメールを送信（Outlook）

    # 接続設定情報

    try:
        os.environ['EMAIL_FROM']
        os.environ['EMAIL_TO']
        os.environ['EMAIL_PASS']
        os.environ['MONGO_SERVER']
        os.environ['MONGO_PORT']
        os.environ['MONGO_USE_DB']
        os.environ['MONGO_USER']
        os.environ['MONGO_PASS']
        # 各コレクション
        os.environ['MONGO_CRAWLER_RESPONSE']
        os.environ['MONGO_SCRAPED_FROM_RESPONSE']
        os.environ['MONGO_NEWS_CLIP_MASTER']
        os.environ['MONGO_CRAWLER_LOGS']
        os.environ['MONGO_CONTROLLER']
        os.environ['MONGO_ASYNCHRONOUS_REPORT']

    except:
        raise CloseSpider('環境変数エラー：不足している環境変数があります。')
