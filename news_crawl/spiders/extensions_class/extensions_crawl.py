import pickle
import scrapy
# import os
# from typing import Any
from datetime import datetime
from scrapy.spiders import CrawlSpider
from scrapy.http import Request
from scrapy.http import Response
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
# from news_crawl.spiders.common.start_request_debug_file_init import start_request_debug_file_init
# from news_crawl.spiders.common.crawling_domain_duplicate_check import CrawlingDomainDuplicatePrevention
from news_crawl.spiders.common.spider_init import spider_init
from news_crawl.spiders.common.spider_closed import spider_closed
from scrapy.spidermiddlewares.httperror import HttpError
from twisted.internet.error import DNSLookupError
from twisted.internet.error import TimeoutError, TCPTimedOutError
# from bs4 import BeautifulSoup as bs4
from bs4.element import ResultSet

class ExtensionsCrawlSpider(CrawlSpider):
    '''
    CrawlSpiderの機能を拡張したクラス。
    (前提)
    name, allowed_domains, start_urls, _domain_name, spider_versionの値は当クラスを継承するクラスで設定すること。
    '''
    name: str = 'extension_crawl'                           # 継承先で上書き要。
    allowed_domains: list = ['sample.com']                      # 継承先で上書き要。
    start_urls: list = ['https://www.sample.com/crawl.html', ]  # 継承先で上書き要。
    custom_settings: dict = {
        'DEPTH_LIMIT': 2,
        'DEPTH_STATS_VERBOSE': True,
    }
    _spider_version: float = 0.0          # spiderのバージョン。継承先で上書き要。
    _extensions_crawl_version: float = 1.0         # 当クラスのバージョン

    # 引数の値保存
    kwargs_save: dict                    # 取得した引数を保存
    # MongoDB関連
    mongo: MongoModel                   # MongoDBへの接続を行うインスタンスをspider内に保持。pipelinesで使用。
    # スパイダーの挙動制御関連、固有の情報など
    _crawl_start_time: datetime         # Scrapy起動時点の基準となる時間
    _domain_name = 'sample_com'         # 各種処理で使用するドメイン名の一元管理。継承先で上書き要。

    # 次回クロールポイント情報
    _next_crawl_point: dict = {}

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
        for url in self.start_urls:
            yield scrapy.Request(
                #url, callback=self._parse, errback=self.errback_handle,  # dont_filter=True
                url, callback=self._parse,  # dont_filter=True
            )

    # def errback_handle(self, failure):
    #     '''
    #     リクエストでエラーがあった場合、エラー情報をログに出力、メールによる通知を行う。
    #     '''
    #     self.logger.error(
    #         '=== start_requestでエラー発生 ', )
    #     request: Request = failure.request
    #     response: Response = failure.value.response
    #     self.logger.error('ErrorType : %s', failure.type)
    #     self.logger.error('request_url : %s', request.url)

    #     title: str = '(error)スパイダー('+self.name+')'
    #     msg: str = '\n'.join([
    #         'スパイダー名 : ' + self.name,
    #         'type : ' + str(failure.type),
    #         'request_url : ' + str(request.url),
    #     ])

    #     if failure.check(HttpError):
    #         self.logger.error('response_url : %s', response.url)
    #         self.logger.error('response_status : %s', response.status)

    #         msg: str = '\n'.join([
    #             msg,
    #             'response_url : ' + str(response.url),
    #             'response_status : ' + str(response.status),
    #         ])
    #     elif failure.check(DNSLookupError):
    #         pass
    #     elif failure.check(TimeoutError, TCPTimedOutError):
    #         pass
    #     else:
    #         pass

    #     mail_send(self, title, msg, self.kwargs_save)

    def parse_news(self, response: Response):
        ''' (拡張メソッド)
        取得したレスポンスよりDBへ書き込み
        '''
        pagination: ResultSet = self.pagination_check(response)
        if len(pagination) > 0:
            self.logger.info(
                '=== parse_news 次のページあり → リクエストに追加 : %s', pagination[0].get('href'))
            #yield scrapy.Request(response.urljoin(pagination[0].get('href')), callback=self.parse_news, errback=self.errback_handle)
            yield scrapy.Request(response.urljoin(pagination[0].get('href')), callback=self.parse_news)

        _info = self.name + ':' + str(self._spider_version) + ' / ' \
            + 'extensions_crawl:' + str(self._extensions_crawl_version)

        yield NewsCrawlItem(
            url=response.url,
            response_time=datetime.now().astimezone(self.settings['TIMEZONE']),
            response_headers=pickle.dumps(response.headers),
            response_body=pickle.dumps(response.body),
            spider_version_info=_info
        )

    def pagination_check(self, response: Response) -> ResultSet:
        '''(拡張メソッド)
        次ページがあれば、BeautifulSoupのResultSetで返す。
        このメソッドは継承先のクラスでオーバーライドして使うことを前提とする。
        '''
        return ResultSet('','')

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

    def pages_setting(self, start_page: int, end_page: int) -> dict:
        ''' (拡張メソッド)
        クロール対象のurlを抽出するページの開始・終了の範囲を決める。\n
        ・起動時の引数にpagesがある場合は、その指定に従う。\n
        ・それ以外は、各サイトの標準値に従う。
        '''
        if 'pages' in self.kwargs_save:
            pages: list = eval(self.kwargs_save['pages'])
            return{'start_page': pages[0], 'end_page': pages[1]}
        else:
            return{'start_page': start_page, 'end_page': end_page}

    # def layout_change_notice(self, response: Response) -> None:
    #     '''
    #     レイアウトの変更が発生した可能性がある場合、メールにて通知する。
    #     '''
    #     layout_change_notice(self, response)
