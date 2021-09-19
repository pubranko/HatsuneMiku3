import pickle
import scrapy
from typing import Any
from datetime import datetime
from scrapy.spiders import XMLFeedSpider
from scrapy.http import Response
from scrapy.http.response.xml import XmlResponse
from scrapy.utils.spider import iterate_spider_output
from news_crawl.items import NewsCrawlItem
from models.mongo_model import MongoModel
from news_crawl.spiders.common.start_request_debug_file_generate import start_request_debug_file_generate
from news_crawl.spiders.common.spider_init import spider_init
from news_crawl.spiders.common.spider_closed import spider_closed


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
        'DEPTH_STATS_VERBOSE': True,
    }
    _spider_version: float = 0.0          # spiderのバージョン。継承先で上書き要。
    _extensions_xml_version: float = 1.0         # 当クラスのバージョン

    # 引数の値保存
    kwargs_save: dict
    # MongoDB関連
    mongo: MongoModel                   # MongoDBへの接続を行うインスタンスをspider内に保持。pipelinesで使用。
    # スパイダーの挙動制御関連、固有の情報など
    _crawling_start_time: datetime         # Scrapy起動時点の基準となる時間
    _domain_name = 'sample_com'         # 各種処理で使用するドメイン名の一元管理。継承先で上書き要。

    # 次回クロールポイント情報
    _crawl_point: dict = {}

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

        spider_init(self, *args, **kwargs)

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(
                url, callback=self._parse,  # dont_filter=True
            )

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
        # サイトマップごとの最大更新時間を記録(controllerコレクションへ保存する内容)
        self._crawl_point[response.url] = {
            'latest_lastmod': self._max_lstmod,
            'latest_url': self._max_url,
            'crawling_start_time': self._crawling_start_time.isoformat()
        }

    def parse_news(self, response: Response):
        ''' (拡張メソッド)
        取得したレスポンスよりDBへ書き込み
        '''
        _info = self.name + ':' + str(self._spider_version) + ' / ' \
            + 'extensions_crawl:' + str(self._extensions_xml_version)

        yield NewsCrawlItem(
            domain=self.allowed_domains[0],
            url=response.url,
            response_time=datetime.now().astimezone(self.settings['TIMEZONE']),
            response_headers=pickle.dumps(response.headers),
            response_body=pickle.dumps(response.body),
            spider_version_info=_info,
            crawling_start_time=self._crawling_start_time,
            sitemap_data=[],
        )

    def closed(self, spider):
        '''spider終了処理'''
        spider_closed(self)

    def _custom_url(self, url: str) -> str:
        ''' (拡張メソッド)
        requestしたいurlをカスタマイズしたい場合、継承先でオーバーライドして使用する。
        '''
        return url
