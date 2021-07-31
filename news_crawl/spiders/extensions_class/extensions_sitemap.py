import pickle
import scrapy
#import os
import re
import scrapy
# from typing import Any
from datetime import datetime, timedelta
from dateutil import parser
from scrapy.spiders import SitemapSpider
from scrapy.http import Request
from scrapy.http import Response
from scrapy.utils.sitemap import Sitemap, sitemap_urls_from_robots
from scrapy.spiders.sitemap import iterloc
# from scrapy.exceptions import CloseSpider
# from scrapy.statscollectors import MemoryStatsCollector
from news_crawl.items import NewsCrawlItem
from news_crawl.models.mongo_model import MongoModel
# from news_crawl.models.crawler_controller_model import CrawlerControllerModel
# from news_crawl.models.crawler_logs_model import CrawlerLogsModel
# from news_crawl.settings import TIMEZONE
# from news_crawl.spiders.common.environ_check import environ_check
# from news_crawl.spiders.common.argument_check import argument_check
from news_crawl.spiders.common.layout_change_notice import layout_change_notice
from news_crawl.spiders.common.mail_send import mail_send
from news_crawl.spiders.common.start_request_debug_file_generate import start_request_debug_file_generate
from news_crawl.spiders.common.term_days_Calculation import term_days_Calculation
# from news_crawl.spiders.common.start_request_debug_file_init import start_request_debug_file_init
# from news_crawl.spiders.common.crawling_domain_duplicate_check import CrawlingDomainDuplicatePrevention
from news_crawl.spiders.common.spider_init import spider_init
from news_crawl.spiders.common.spider_closed import spider_closed
from scrapy.spidermiddlewares.httperror import HttpError
from twisted.internet.error import DNSLookupError
from twisted.internet.error import TimeoutError, TCPTimedOutError


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
        'DEPTH_STATS_VERBOSE': True,
    }
    _spider_version: float = 0.0          # spiderのバージョン。継承先で上書き要。
    _extensions_sitemap_version: float = 1.0         # 当クラスのバージョン

    # 引数の値保存
    kwargs_save: dict
    # MongoDB関連
    mongo: MongoModel                   # MongoDBへの接続を行うインスタンスをspider内に保持。pipelinesで使用。
    # スパイダーの挙動制御関連、固有の情報など
    _crawl_start_time: datetime         # Scrapy起動時点の基準となる時間
    _domain_name = 'sample_com'         # 各種処理で使用するドメイン名の一元管理。継承先で上書き要。

    # 次回クロールポイント情報
    _next_crawl_point: dict = {}
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

        spider_init(self, *args, **kwargs)
        # # 必要な環境変数チェック
        # environ_check()
        # # MongoDBオープン
        # self.mongo = MongoModel()
        # # 前回のドメイン別のクロール結果を取得
        # _crawler_controller = CrawlerControllerModel(self.mongo)
        # self._next_crawl_point = _crawler_controller.next_crawl_point_get(
        #     self._domain_name, self.name)

        # # 引数保存・チェック
        # self.kwargs_save: dict = kwargs
        # argument_check(
        #     self, self._domain_name, self._next_crawl_point, *args, **kwargs)

        # # 同一ドメインへの多重クローリングを防止
        # self.crawling_domain_control = CrawlingDomainDuplicatePrevention()
        # duplicate_check = self.crawling_domain_control.execution(
        #     self._domain_name)
        # if not duplicate_check:
        #     raise CloseSpider('同一ドメインへの多重クローリングとなるため中止')

        # # クロール開始時間
        # if 'crawl_start_time' in self.kwargs_save:
        #     self._crawl_start_time = self.kwargs_save['crawl_start_time']
        # else:
        #     self._crawl_start_time = datetime.now().astimezone(
        #         TIMEZONE)

        # self.logger.info(
        #     '=== __init__ : 開始時間(%s)' % (self._crawl_start_time.isoformat()))
        # self.logger.info(
        #     '=== __init__ : 引数(%s)' % (kwargs))
        # self.logger.info(
        #     '=== __init__ : 今回向けクロールポイント情報 \n %s', self._next_crawl_point)

        # start_request_debug_file_init(self, self.kwargs_save)

    def start_requests(self):
        for url in self.sitemap_urls:
            yield scrapy.Request(
                url, callback=self._parse_sitemap, errback=self.errback_handle,  # dont_filter=True
            )

    def _parse_sitemap(self, response):
        '''（仕方なくオーラーライド）
        Requestにエラーハンドルを追加。
        '''
        if response.url.endswith('/robots.txt'):
            for url in sitemap_urls_from_robots(response.text, base_url=response.url):
                yield Request(url, callback=self._parse_sitemap, errback=self.errback_handle)
        else:
            body = self._get_sitemap_body(response)
            if body is None:
                self.logger.warning("Ignoring invalid sitemap: %(response)s",
                                    {'response': response}, extra={'spider': self})
                return

            s = Sitemap(body)
            it = self.sitemap_filter(s)

            if s.type == 'sitemapindex':
                for loc in iterloc(it, self.sitemap_alternate_links):
                    if any(x.search(loc) for x in self._follow):
                        yield Request(loc, callback=self._parse_sitemap, errback=self.errback_handle)
            elif s.type == 'urlset':
                for loc in iterloc(it, self.sitemap_alternate_links):
                    for r, c in self._cbs:
                        if r.search(loc):
                            yield Request(loc, callback=c, errback=self.errback_handle)
                            break

    def errback_handle(self, failure):
        '''
        リクエストでエラーがあった場合、エラー情報をログに出力、メールによる通知を行う。
        '''
        self.logger.error(
            '=== start_requestでエラー発生 ', )
        request: Request = failure.request
        response: Response = failure.value.response
        self.logger.error('ErrorType : %s', failure.type)
        self.logger.error('request_url : %s', request.url)

        title: str = '(error)スパイダー('+self.name+')'
        msg: str = '\n'.join([
            'スパイダー名 : ' + self.name,
            'type : ' + str(failure.type),
            'request_url : ' + str(request.url),
        ])

        if failure.check(HttpError):
            self.logger.error('response_url : %s', response.url)
            self.logger.error('response_status : %s', response.status)

            msg: str = '\n'.join([
                msg,
                'response_url : ' + str(response.url),
                'response_status : ' + str(response.status),
            ])
        elif failure.check(DNSLookupError):
            pass
        elif failure.check(TimeoutError, TCPTimedOutError):
            pass
        else:
            pass

        mail_send(self, title, msg, self.kwargs_save)

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
                self._next_crawl_point[sitemap_url]['latest_lastmod'])

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
                        and self._next_crawl_point[sitemap_url]['latest_url']:
                    _crwal_flg = False

            if _crwal_flg:
                if self._custom_url_flg:
                    _entry['loc'] = self._custom_url(_entry)

                yield _entry

        # サイトマップごとの最大更新時間を記録(crawler_controllerコレクションへ保存する内容)
        self._next_crawl_point[sitemap_url] = {
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
        '''spider終了処理'''
        spider_closed(self)
        # _crawler_controller = CrawlerControllerModel(self.mongo)
        # _crawler_controller.next_crawl_point_update(
        #     self._domain_name, self.name, self._next_crawl_point)

        # self.logger.info(
        #     '=== closed : crawler_controllerに次回クロールポイント情報を保存 \n %s', self._next_crawl_point)

        # stats: MemoryStatsCollector = self.crawler.stats
        # crawler_logs = CrawlerLogsModel(self.mongo)
        # crawler_logs.insert_one({
        #     'crawl_start_time': self._crawl_start_time.isoformat(),
        #     'record_type': 'spider_stats',
        #     'domain_name': self._domain_name,
        #     'self_name': self.name,
        #     'stats': stats.get_stats(),
        # })

        # self.mongo.close()
        # self.logger.info('=== Spider closed: %s', self.name)

    def _custom_url(self, url: dict) -> str:
        ''' (拡張メソッド)
        requestしたいurlをカスタマイズしたい場合、継承先でオーバーライドして使用する。
        '''
        return url['url']

    def layout_change_notice(self, response: Response) -> None:
        '''
        レイアウトの変更が発生した可能性がある場合、メールにて通知する。
        '''
        layout_change_notice(self, response)
