from typing import Any
from scrapy.spiders import SitemapSpider
from datetime import datetime, timedelta
from dateutil import parser
import re
import sys
import pickle
from news_crawl.items import NewsCrawlItem
from news_crawl.models.mongo_model import MongoModel
from news_crawl.models.crawler_controller_model import CrawlerControllerModel


class SankeiComSitemapSpider(SitemapSpider):
    name = 'sankei_com_sitemap'
    allowed_domains = ['sankei.com']
    sitemap_urls = ['https://www.sankei.com/sitemap.xml', ]
    custom_settings = {
        'DEPTH_LIMIT': 2,
        'DEPTH_STATS_VERBOSE': True
    }
    # 引数の値保存
    __lastmod_recent_time: int = 0  # 直近の15分など。分単位で指定することにしよう。0は制限なしとする。
    __url_pattern: str = ''         # 指定したパターンをurlに含むもので限定(正規表現)
    __continued: bool = False       # 前回の続きから(最後に取得したlastmodの日時)
    debug_flg = False               # debugモードを引数で指定された場合True
    # MongoDB関連
    mongo: MongoModel               # MongoDBへの接続を行うインスタンスをspider内に保持。pipelinesで使用。
    crawler_controller_recode: Any  # crawler_controllerコレクションから取得した対象ドメインのレコード
    # スパイダーの挙動制御関連、固有の条法など
    __until_this_time: datetime     # sitemapで対象とするlastmodに制限をかける時間
    __crawl_start_time: datetime    # Scrapy起動時点の基準となる時間
    crawl_start_time_iso: str       # Scrapy起動時点の基準となる時間(ISOフォーマットの文字列)
    # sitemapで最初の1件目にあるlastmod(最新の更新)を保持。pipelineで使用。
    latest_lastmod: str
    latest_url: str                 # 上記'latest_lastmod'のurl
    domain_name = 'sankei_com'      # 各種処理で使用するドメイン名の一元管理
    spider_type = 'SitemapSpider'   # spiderの種類。pipelineで使用。

    def __init__(self, *args, **kwargs):
        super(SankeiComSitemapSpider, self).__init__(*args, **kwargs)
        self.mongo = MongoModel()
        #引数チェック・保存
        #単項目チェック
        if 'debug' in kwargs:
            if kwargs['debug'] == 'Yes':
                self.debug_flg = True
                print('=== debugモード ON ===')
        if 'url_pattern' in kwargs:
            self.__url_pattern = kwargs['url_pattern']
        if 'lastmod_recent_time' in kwargs:
            if kwargs['lastmod_recent_time'].isdecimal():
                self.__lastmod_recent_time = int(kwargs['lastmod_recent_time'])
            else:
                sys.exit('引数エラー：lastmod_recent_timeは数字(0〜9)のみ使用可。分単位で指定してください。')
        if 'continued' in kwargs:
            if kwargs['continued'] == 'Yes':
                self.crawler_controller = CrawlerControllerModel(self.mongo)
                self.crawler_controller_recode = self.crawler_controller.find_one(
                    {'domain': self.domain_name})
                if self.crawler_controller_recode == None:
                    sys.exit('引数エラー：domain = ' + self.domain_name +
                             ' は前回のcrawl情報がありません。初回から"continued"の使用は不可です。')
                else:
                    self.__continued = True
            else:
                sys.exit('引数エラー：continuedに使用できるのは、"Yes"のみです。')
        #関連チェック
        if 'lastmod_recent_time' in kwargs and 'continued' in kwargs:
            sys.exit('引数エラー：lastmod_recent_timeとcontinuedは同時には使えません。')

    def sitemap_filter(self, entries):
        '''
        親クラスのSitemapSpiderの同名メソッドをオーバーライド。
        entriesには、サイトマップから取得した情報(loc,lastmodなど）が辞書形式で格納されている。
        例)entryの使い方：entry['lastmod'],entry['loc'],entry.items()
        サイトマップから取得したurlへrequestを行う前に条件で除外することができる。
        対象のurlの場合、”yield entry”で返すと親クラス側でrequestされる。
        '''
        # 1件目のlastmod（最新の更新）とurlを取得
        entry: dict = next(iter(entries))
        self.latest_lastmod = entry['lastmod']
        self.latest_url = entry['loc']

        # sitemap調査用。debugモードの場合のみ。
        if self.debug_flg:
            file = open('debug_entries.txt', 'w')
            for entry in entries:
                file.write(entry['lastmod'] + ', ' + entry['loc'] + '\n')
            file.close()

        self.__crawl_start_time = datetime.now().astimezone(
            self.settings['TIMEZONE'])
        self.crawl_start_time_iso = self.__crawl_start_time.isoformat()

        if self.__lastmod_recent_time != 0:
            self.__until_this_time = self.__crawl_start_time - \
                timedelta(minutes=self.__lastmod_recent_time)
        if self.__continued:
            self.__until_this_time = parser.parse(
                self.crawler_controller_recode['spider_name'][self.name]['latest_lastmod']).astimezone(self.settings['TIMEZONE'])

        for entry in entries:
            '''
            引数に絞り込み指定がある場合、その条件を満たす場合のみ対象とする。
            '''
            crwal_flg: bool = True
            date_lastmod = parser.parse(entry['lastmod']).astimezone(
                self.settings['TIMEZONE'])

            if self.__url_pattern != '':                    # url絞り込み指定あり
                pattern = re.compile(self.__url_pattern)
                if pattern.search(entry['loc']) == None:
                    crwal_flg = False
            if self.__lastmod_recent_time != 0:             # lastmod絞り込み指定あり
                if date_lastmod < self.__until_this_time:
                    crwal_flg = False
            if self.__continued:                            # 前回クロールからの続きの場合
                if date_lastmod < self.__until_this_time:
                    crwal_flg = False
                elif date_lastmod == self.__until_this_time \
                        and self.crawler_controller_recode['spider_name'][self.name]['latest_url']:
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
        )  # ログに不要な値を出さないようitems.pyで細工して文字列を短縮。
