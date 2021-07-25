from news_crawl.spiders.common.mail_send import mail_send
from scrapy.spiders import Spider
from scrapy.http import Response


def layout_change_notice(spider,response:Response) -> None:
    '''
    レイアウトの変更が発生した可能性がある場合、メールにて通知する。
    '''
    title = 'レイアウト変更の可能性あり(' + str(spider.name) + ')'
    msg: str = '\n'.join([
    'スパイダー名 : ' + str(spider.name),
    'url : ' + response.url,
    'crawl_start_time : ' + spider._crawl_start_time.isoformat()
    ])
    mail_send(spider, title, msg, spider.kwargs_save)
