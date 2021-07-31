from news_crawl.spiders.common.mail_send import mail_send
from scrapy.http import Response
from datetime import datetime

def layout_change_notice(spider,response:Response) -> None:
    '''
    レイアウトの変更が発生した可能性がある場合、メールにて通知する。
    '''
    name:str = spider.name
    crawl_start_time:datetime = spider._crawl_start_time()
    title = 'レイアウト変更の可能性あり(' + name + ')'
    msg: str = '\n'.join([
    'スパイダー名 : ' + name,
    'url : ' + response.url,
    'crawl_start_time : ' + crawl_start_time.isoformat()
    ])
    mail_send(title, msg,)
