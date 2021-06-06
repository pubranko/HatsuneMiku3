import pickle
import re
import scrapy
from typing import Any
from datetime import datetime, timedelta
from dateutil import parser
from scrapy.spiders import SitemapSpider
from scrapy.http import Response
from scrapy.utils.sitemap import Sitemap
from news_crawl.items import NewsCrawlItem
from news_crawl.models.mongo_model import MongoModel
from news_crawl.models.crawler_controller_model import CrawlerControllerModel
from news_crawl.settings import TIMEZONE
from news_crawl.spiders.function.argument_check import argument_check
from news_crawl.spiders.function.start_request_debug_file_generate import start_request_debug_file_generate
from news_crawl.spiders.function.term_days_Calculation import term_days_Calculation
from news_crawl.spiders.function.start_request_debug_file_init import start_request_debug_file_init
from scrapy.spidermiddlewares.httperror import HttpError
from twisted.internet.error import DNSLookupError
from twisted.internet.error import TimeoutError, TCPTimedOutError
from twisted.python.failure import Failure

import smtplib
from email.mime.text import MIMEText
import ssl

class ExtensionsSitemapSpider(SitemapSpider):
    '''
    SitemapSpiderの機能を拡張したクラス。
    (前提)
    name, allowed_domains, sitemap_urls, _domain_name, spider_versionの値は当クラスを継承するクラスで設定すること。
    sitemap_filter()メソッドのオーバーライドも継承先のクラスで行うこと。
    '''
    name: str = 'extension_sitemap'                                 # 継承先で上書き要。
    allowed_domains: list = ['sample.com']                          # 継承先で上書き要。
    sitemap_urls: list = ['https://www.sample.com/sitemap.xml', ]   # 継承先で上書き要。
    custom_settings: dict = {
        'DEPTH_LIMIT': 2,
        'DEPTH_STATS_VERBOSE': True
    }
    _spider_version: float = 0.0          # spiderのバージョン。継承先で上書き要。
    _extensions_sitemap_version: float = 1.0         # 当クラスのバージョン

    # 引数の値保存
    kwargs_save: dict
    # MongoDB関連
    mongo: MongoModel                   # MongoDBへの接続を行うインスタンスをspider内に保持。pipelinesで使用。
    _crawler_controller_recode: Any     # crawler_controllerコレクションから取得した対象ドメインのレコード
    # スパイダーの挙動制御関連、固有の情報など
    _crawl_start_time: datetime         # Scrapy起動時点の基準となる時間
    _domain_name = 'sample_com'         # 各種処理で使用するドメイン名の一元管理。継承先で上書き要。

    # crawler_controllerコレクションへ書き込むレコードのdomain以降のレイアウト雛形。※最上位のKeyのdomainはサイトの特性にかかわらず固定とするため。
    _next_crawl_info: dict = {name: {}, }
    # sitemap_urlsに複数のサイトマップを指定した場合、その数だけsitemap_filterが可動する。その際、どのサイトマップか判別できるように処理中のサイトマップと連動するカウント。
    _sitemap_urls_count: int = 0
    # sitemapのリンク先urlをカスタマイズしたい場合、継承先のクラスでTrueにする。
    # Trueの場合、継承先でオーバーライドしたcustom_url()メソッドを使い、urlをカスタムする。
    _custom_url_flg: bool = False

    def __init__(self, *args, **kwargs):
        ''' (拡張メソッド)
        親クラスの__init__処理後に追加で初期処理を行う。
        '''
        super().__init__(*args, **kwargs)
        # MongoDBオープン
        self.mongo = MongoModel()
        # 前回のドメイン別のクロール結果を取得
        _crawler_controller = CrawlerControllerModel(self.mongo)
        self._crawler_controller_recode = _crawler_controller.find_one(
            {'domain': self._domain_name})
        self.logger.info(
            '=== __init__ : crawler_controllerにある前回情報 \n %s', self._crawler_controller_recode)
        # 前回のクロール情報を次回向けの初期値とする。
        self._next_crawl_info: dict = {self.name: {}, }
        if not self._crawler_controller_recode == None:
            if self.name in self._crawler_controller_recode:
                self._next_crawl_info[self.name] = self._crawler_controller_recode[self.name]

        # 引数保存・チェック
        self.kwargs_save: dict = kwargs
        argument_check(
            self, self._domain_name, self._crawler_controller_recode, *args, **kwargs)

        start_request_debug_file_init(self, self.kwargs_save)

        # クロール開始時間
        self._crawl_start_time = datetime.now().astimezone(
            TIMEZONE)

    def start_requests(self):
        for url in self.sitemap_urls:
            yield scrapy.Request(
                url, callback=self._parse_sitemap, errback=self.errback_handle, dont_filter=True
            )

    def errback_handle(self, failure):
        self.logger.error(
            '=== start_requestでエラー発生 ', )

        if failure.check(HttpError):
            response = failure.value.response
            self.logger.error('ErrorType : %s', failure.type)
            self.logger.error('HttpError on %s', response.url)
            self.logger.error('HttpError on %s', response.status)

            # s = smtplib.SMTP('localhost')
            # s.sendmail('test@test.com',['mikuras3@outlook.com'],'test!')
            # s.close()
            # SMTP サーバー名: smtp-mail.outlook.com
            # SMTP ポート: 587
            # SMTP 暗号化方法 STARTTLS

            # server = smtplib.SMTP('smtp-mail.outlook.com',587)
            # server.login('mikuras3@outlook.com','Pannda@te10')
            # server.set_debuglevel(True)
            # if server.has_extn('STARTTLS'):
            #     server.starttls()

            # server = smtplib.SMTP_SSL('smtp-mail.outlook.com',587, context=ssl.create_default_context())
            # # server.sendmail('smtp-mail.outlook.com',587,['mikuras3@outlook.com'],'test!')
            # server.close()

        elif failure.check(DNSLookupError):
            request = failure.request
            self.logger.error('ErrorType : %s', failure.type)
            self.logger.error('DNSLookupError on %s', request.url)
        elif failure.check(TimeoutError, TCPTimedOutError):
            request = failure.request
            self.logger.error('ErrorType : %s', failure.type)
            self.logger.error('TimeoutError on %s', request.url)
        else:
            request = failure.request
            self.logger.error('ErrorType : %s', failure.type)
            self.logger.error(request.url)

    def sitemap_filter(self, entries: Sitemap):
        '''
        親クラスのSitemapSpiderの同名メソッドをオーバーライド。
        entriesには、サイトマップから取得した情報(loc,lastmodなど）が辞書形式で格納されている。
        例)entryの使い方：entry['lastmod'],entry['loc'],entry.items()
        サイトマップから取得したurlへrequestを行う前に条件で除外することができる。
        対象のurlの場合、”yield entry”で返すと親クラス側でrequestされる。
        '''
        sitemap_url = self.sitemap_urls[self._sitemap_urls_count]

        # ExtensionsSitemapSpiderクラスを継承した場合のsitemap_filter共通処理
        start_request_debug_file_generate(
            self.name, sitemap_url, entries, self.kwargs_save)

        # 直近の数分間の指定がある場合
        until_this_time: datetime = self._crawl_start_time
        if 'lastmod_recent_time' in self.kwargs_save:
            until_this_time = until_this_time - \
                timedelta(minutes=int(self.kwargs_save['lastmod_recent_time']))
            self.logger.info(
                '=== sitemap_filter : lastmod_recent_timeより計算した時間 %s', until_this_time.isoformat())

        # urlに含まれる日付に指定がある場合
        _url_term_days_list: list = []
        if 'url_term_days' in self.kwargs_save:   #
            _url_term_days_list = term_days_Calculation(
                self._crawl_start_time, int(self.kwargs_save['url_term_days']), '%y%m%d')
            self.logger.info(
                '=== sitemap_filter : url_term_daysより計算した日付 %s', ', '.join(_url_term_days_list))

        # 前回からの続きの指定がある場合
        _last_time: datetime = datetime.now()  # 型ヒントエラー回避用の初期値
        if 'continued' in self.kwargs_save:
            _last_time = parser.parse(
                self._crawler_controller_recode[self.name][sitemap_url]['latest_lastmod'])

        # 処理中のサイトマップ内で、最大のlastmodとurlを記録するエリア
        _max_lstmod: str = ''
        _max_url: str = ''

        for _entry in entries:
            '''
            引数に絞り込み指定がある場合、その条件を満たす場合のみ対象とする。
            '''
            _entry: dict

            if _max_lstmod < _entry['lastmod']:
                _max_lstmod = _entry['lastmod']
                _max_url = _entry['loc']

            _crwal_flg: bool = True
            _date_lastmod = parser.parse(_entry['lastmod']).astimezone(
                self.settings['TIMEZONE'])

            if 'url_pattern' in self.kwargs_save:   # url絞り込み指定あり
                pattern = re.compile(self.kwargs_save['url_pattern'])
                if pattern.search(_entry['loc']) == None:
                    _crwal_flg = False
            if 'url_term_days' in self.kwargs_save:                       # 期間指定あり
                _pattern = re.compile('|'.join(_url_term_days_list))
                if _pattern.search(_entry['loc']) == None:
                    _crwal_flg = False
            if 'lastmod_recent_time' in self.kwargs_save:             # lastmod絞り込み指定あり
                if _date_lastmod < until_this_time:
                    _crwal_flg = False
            if 'continued' in self.kwargs_save:
                if _date_lastmod < _last_time:
                    _crwal_flg = False
                elif _date_lastmod == _last_time \
                        and self._crawler_controller_recode[self.name][sitemap_url]['latest_url']:
                    _crwal_flg = False

            if _crwal_flg:
                if self._custom_url_flg:
                    _entry['loc'] = self._custom_url(_entry)

                yield _entry

        # サイトマップごとの最大更新時間を記録(crawler_controllerコレクションへ保存する内容)
        self._next_crawl_info[self.name][sitemap_url] = {
            'latest_lastmod': _max_lstmod,
            'latest_url': _max_url,
            'crawl_start_time': self._crawl_start_time.isoformat(),
        }
        self._sitemap_urls_count += 1  # 次のサイトマップurl用にカウントアップ

    def parse(self, response: Response):
        '''
        取得したレスポンスよりDBへ書き込み
        '''
        _info = self.name + ':' + str(self._spider_version) + ' / ' \
            + 'extensions_sitemap:' + str(self._extensions_sitemap_version)

        yield NewsCrawlItem(
            url=response.url,
            response_time=datetime.now().astimezone(self.settings['TIMEZONE']),
            response_headers=pickle.dumps(response.headers),
            response_body=pickle.dumps(response.body),
            spider_version_info=_info
        )

    def closed(self, spider):
        '''
        spider終了時、次回クロール向けの情報をcrawler_controllerへ記録する。
        '''
        crawler_controller = CrawlerControllerModel(self.mongo)
        self._crawler_controller_recode = crawler_controller.find_one(
            {'domain': self._domain_name})

        if self._crawler_controller_recode == None:  # ドメインに対して初クロールの場合
            self._crawler_controller_recode = {
                'domain': self._domain_name,
                self.name: self._next_crawl_info[self.name]
            }
        else:
            self._crawler_controller_recode[self.name] = self._next_crawl_info[self.name]

        crawler_controller.update(
            {'domain': self._domain_name},
            self._crawler_controller_recode,
        )

        self.logger.info(
            '=== closed : crawler_controllerに次回情報を保存 \n %s', self._crawler_controller_recode)

        self.mongo.close()
        self.logger.info('=== Spider closed: %s', self.name)

    def _custom_url(self, url: dict) -> str:
        ''' (拡張メソッド)
        requestしたいurlをカスタマイズしたい場合、継承先でオーバーライドして使用する。
        '''
        return url['url']
