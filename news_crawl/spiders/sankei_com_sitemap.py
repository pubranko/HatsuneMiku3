from logging import NullHandler
from scrapy.spiders import SitemapSpider
from datetime import datetime, timedelta
from dateutil import parser
import re
import pickle
from news_crawl.items import NewsCrawlItem
from news_crawl.models.mongo_model import MongoModel


class SankeiComSitemapSpider(SitemapSpider):
    name = 'sankei_com_sitemap'
    allowed_domains = ['sankei.com']
    sitemap_urls = ['https://www.sankei.com/robots.txt', ]
    custom_settings = {
        'DEPTH_LIMIT': 2,
        'DEPTH_STATS_VERBOSE': True
    }
    __lastmod_recent_time: int = 0  # 直近の15分など。分単位で指定することにしよう。0は制限なしとする。
    __start_time: datetime  # Scrapy起動時点の基準となる時間
    __recent_time_limit: datetime  # lastmod_recent_timeとnowから計算した制限をかける時間
    __url_pattern: str = ''  # 指定したパターンをurlに含むもので限定
    __continued: bool = False  # 前回の続きから(最後に取得したlastmodの日時) ※実装は後回し
    mongo:MongoModel

    def __init__(self, *args, **kwargs):
        super(SankeiComSitemapSpider, self).__init__(*args, **kwargs)
        if 'lastmod_recent_time' in kwargs:
            self.__lastmod_recent_time = int(kwargs['lastmod_recent_time'])
        if 'url_pattern' in kwargs:
            self.__url_pattern = kwargs['url_pattern']
        if 'continued' in kwargs:
            self.__continued = kwargs['continued']

        self.mongo = MongoModel()

    def sitemap_filter(self, entries):
        '''
        親クラスのSitemapSpiderの同名メソッドをオーバーライド。
        entriesには、サイトマップから取得した情報(loc,lastmodなど）が辞書形式で格納されている。
        例)entryの使い方：entry['lastmod'],entry['loc'],entry.items()
        サイトマップから取得したurlへrequestを行う前に条件で除外することができる。
        対象のurlの場合、”yield entry”で返すと親クラス側でrequestされる。
        '''
        self.__start_time = datetime.now().astimezone(
            self.settings['TIMEZONE'])
        self.__recent_time_limit = self.__start_time - \
            timedelta(minutes=self.__lastmod_recent_time)

        for entry in entries:
            '''
            引数に絞り込み指定がある場合、その条件を満たす場合のみ対象とする。
            '''
            crwal_flg: bool = True
            if self.__lastmod_recent_time != 0:  # lastmod絞り込み指定あり
                date_lastmod = parser.parse(entry['lastmod']).astimezone(
                    self.settings['TIMEZONE'])
                if date_lastmod < self.__recent_time_limit:
                    crwal_flg = False
            if self.__url_pattern != '':  # url絞り込み指定あり
                pattern = re.compile(self.__url_pattern)
                if pattern.search(entry['loc']) == None:
                    crwal_flg = False

            if crwal_flg:
                yield entry

    def parse(self, response):
        '''
        取得したレスポンスよりDBへ書き込み
        '''
        response_time = datetime.now()
        yield NewsCrawlItem(
            url=response.url,
            response_time=response_time,
            response_headers=pickle.dumps(response.headers),
            response_body=pickle.dumps(response.body),
        )  # ヘッダーとボディーは文字列が長くログが読めなくなるため、items.pyのNewsclipItemに細工して文字列を短縮。
