import pickle
import scrapy
from typing import Any
from datetime import datetime
from scrapy.spiders import XMLFeedSpider
from scrapy.http import Response
from scrapy.http.response.xml import XmlResponse
from scrapy.utils.spider import iterate_spider_output
from news_crawl.items import NewsCrawlItem
from news_crawl.models.mongo_model import MongoModel
from news_crawl.models.crawler_controller_model import CrawlerControllerModel
from news_crawl.settings import TIMEZONE
from news_crawl.spiders.function.environ_check import environ_check
from news_crawl.spiders.function.argument_check import argument_check
from news_crawl.spiders.function.layout_change_notice import layout_change_notice
from news_crawl.spiders.function.mail_send import mail_send
from news_crawl.spiders.function.start_request_debug_file_generate import start_request_debug_file_generate
from news_crawl.spiders.function.start_request_debug_file_init import start_request_debug_file_init
from scrapy.spidermiddlewares.httperror import HttpError
from twisted.internet.error import DNSLookupError
from twisted.internet.error import TimeoutError, TCPTimedOutError


class ExtensionsXmlFeedSpider(XMLFeedSpider):
    '''
    XMLFeedSpiderの機能を拡張したクラス。
    (前提)
    name, allowed_domains, start_urls, _domain_name, spider_versionの値は当クラスを継承するクラスで設定すること。
    '''
    name: str = 'sample_com_xml_feed'                           # 継承先で上書き要。
    allowed_domains: list = ['sample.com']                      # 継承先で上書き要。
    start_urls: list = ['https://www.sample.com/sitemap.xml', ]  # 継承先で上書き要。
    custom_settings: dict = {
        'DEPTH_LIMIT': 2,
        'DEPTH_STATS_VERBOSE': True
    }
    _spider_version: float = 0.0          # spiderのバージョン。継承先で上書き要。
    _extensions_xml_version: float = 1.0         # 当クラスのバージョン

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

    _request_list: list = []
    # Trueの場合、継承先でオーバーライドしたcustom_url()メソッドを使い、urlをカスタムする。
    _custom_url_flg: bool = False
    # 処理中のサイトマップ内で、最大のlastmodとurlを記録するエリア
    _max_lstmod: str = ''
    _max_url: str = ''
    _xml_extract_count: int = 0
    _entries: list = []

    iterator: str = 'iternodes'
    itertag: str = 'url'

    def __init__(self, *args, **kwargs):
        ''' (拡張メソッド)
        親クラスの__init__処理後に追加で初期処理を行う。
        '''
        super().__init__(*args, **kwargs)
        # 必要な環境変数チェック
        environ_check()
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
        for url in self.start_urls:
            yield scrapy.Request(
                url, callback=self._parse, errback=self.errback_handle,  # dont_filter=True
            )

    def errback_handle(self, failure):
        self.logger.error(
            '=== start_requestでエラー発生 ', )
        request = failure.request
        response = failure.value.response
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

    def parse_nodes(self, response: XmlResponse, nodes):
        """(オーバーライド)
        各xmlファイルの初期処理、主処理、終了処理を記述可能
        """
        # 初期処理
        self._xml_extract_count = 0  # xmlごとに何件対象となったか確認するためのカウンター初期化
        self._entries = []
        # 主処理
        for selector in nodes:
            ret: Any = iterate_spider_output(
                self.parse_node(response, selector))
            for result_item in self.process_results(response, ret):
                yield result_item
        # 終了処理
        start_request_debug_file_generate(
            self.name, response.url, self._entries, self.kwargs_save)

        self.logger.info(
            '=== parse_nodes : XMLの解析完了 : 件数 = %s ,url = %s ', self._xml_extract_count, response.url)
        # サイトマップごとの最大更新時間を記録(crawler_controllerコレクションへ保存する内容)
        self._next_crawl_info[self.name][response.url] = {
            'latest_lastmod': self._max_lstmod,
            'latest_url': self._max_url,
            'crawl_start_time': self._crawl_start_time.isoformat()
        }

    def parse_news(self, response: Response):
        ''' (拡張メソッド)
        取得したレスポンスよりDBへ書き込み
        '''
        _info = self.name + ':' + str(self._spider_version) + ' / ' \
            + 'extensions_crawl:' + str(self._extensions_xml_version)

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

    def _custom_url(self, url: str) -> str:
        ''' (拡張メソッド)
        requestしたいurlをカスタマイズしたい場合、継承先でオーバーライドして使用する。
        '''
        return url

    def layout_change_notice(self,response:Response) -> None:
        layout_change_notice(self,response)
