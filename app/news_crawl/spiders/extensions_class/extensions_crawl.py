import pickle
import scrapy
from typing import Any, Final
from datetime import datetime
from scrapy.spiders import CrawlSpider
from scrapy.http import TextResponse
from urllib.parse import unquote
from scrapy_splash import SplashRequest
#
from BrownieAtelierMongo.collection_models.mongo_model import MongoModel
from BrownieAtelierMongo.collection_models.controller_model import ControllerModel
from BrownieAtelierMongo.collection_models.crawler_logs_model import CrawlerLogsModel
from BrownieAtelierMongo.collection_models.crawler_response_model import CrawlerResponseModel
from news_crawl.items import NewsCrawlItem
from news_crawl.news_crawl_input import NewsCrawlInput
from news_crawl.spiders.common.spider_init import spider_init
from news_crawl.spiders.common.spider_closed import spider_closed
from news_crawl.spiders.common.lastmod_term_skip_check import LastmodTermSkipCheck
from news_crawl.spiders.common.lastmod_continued_skip_check import LastmodContinuedSkipCheck
from news_crawl.spiders.common.urls_continued_skip_check import UrlsContinuedSkipCheck
from news_crawl.spiders.common.pagination_check import PaginationCheck


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

    # MongoDB関連
    mongo: MongoModel                   # MongoDBへの接続を行うインスタンスをspider内に保持。pipelinesで使用。
    # スパイダーの挙動制御関連、固有の情報など
    _domain_name = 'sample_com'         # 各種処理で使用するドメイン名の一元管理。継承先で上書き要。

    # 次回クロールポイント情報 (オーバーライド必須)
    # 各クローラー側でオーバーライドしないと、複数のクローラーでこのdictを共有してしまいます。
    _crawl_point: dict = {}
    '''オーバーライド必須 この説明がvscodeで見えているということは、オーバーライドが漏れています。'''

    # seleniumモード
    selenium_mode: bool = False
    # splashモード
    splash_mode: bool = False

    # 一覧ページの情報を保存 [{'source_url': '', 'lastmod': '', 'loc': ''},,,]
    crawl_urls_list: list[dict[str, Any]] = []

    # クロール対象となったurlリスト[response.url,,,]
    crawl_target_urls: list[str] = []

    # 引数用クラス
    news_crawl_input: NewsCrawlInput
    # 引数による抽出処理のためのクラス
    crawling_continued: LastmodContinuedSkipCheck
    lastmod_term: LastmodTermSkipCheck
    url_continued: UrlsContinuedSkipCheck
    lastmod_continued: LastmodContinuedSkipCheck
    # ページネーションチェック: 次ページがある場合、そのURLを取得する
    pagination_check: PaginationCheck

    ###########
    # 定数
    ###########
    '''当クラスのバージョン管理用のKEY項目名'''
    EXTENSIONS_CRAWL: str = 'extensions_crawl'

    ##################################################
    # 定数 (CrawlerLogsModelのspider_reportと同期)
    ##################################################
    '''クロール対象がある一覧ページURL'''
    CRAWL_URLS_LIST__SOURCE_URL: str = CrawlerLogsModel.CRAWL_URLS_LIST__SOURCE_URL
    '''クロール対象URL'''
    CRAWL_URLS_LIST__LOC: str = CrawlerLogsModel.CRAWL_URLS_LIST__LOC
    '''クロールページの最終更新時間'''
    CRAWL_URLS_LIST__LASTMOD: str = CrawlerLogsModel.CRAWL_URLS_LIST__LASTMOD

    #############################################################
    # 定数 (ControllerModelの レコードタイプcrawl_pointと同期)
    #############################################################
    CRAWL_POINT__LOC: Final[str] = ControllerModel.LOC
    CRAWL_POINT__URLS: Final[str] = ControllerModel.URLS
    CRAWL_POINT__LASTMOD: Final[str] = ControllerModel.LASTMOD
    CRAWL_POINT__LATEST_LASTMOD: Final[str] = ControllerModel.LATEST_LASTMOD
    CRAWL_POINT__CRAWLING_START_TIME: Final[str] = ControllerModel.CRAWLING_START_TIME

    def __init__(self, *args, **kwargs):
        ''' (拡張メソッド)
        親クラスの__init__処理後に追加で初期処理を行う。
        '''
        super().__init__(*args, **kwargs)

        spider_init(self, *args, **kwargs)
        # Extensionsクラス変数を初期化。インスタンス生成時に初期化しないと各スパイダーで変数を共有してしまう。
        self._crawl_point = {}
        self.crawl_urls_list = []
        self.crawl_target_urls = []

        self.pagination_check = PaginationCheck()

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(
                url, callback=self._parse,  # dont_filter=True
            )

    def parse_news(self, response: TextResponse):
        ''' (拡張メソッド)
        取得したレスポンスよりDBへ書き込み
        '''
        # selenium、splash、通常モードにより処理を切り分ける
        meta = {}
        args = {}

        urls: set = set()
        req: list = []
        # ページ内の全リンクを抽出（重複分はsetで削除）
        for link in set(response.css('[href]::attr(href)').getall()):
            # 相対パスの場合絶対パスへ変換。また%エスケープされたものはUTF-8へ変換
            link_url: str = unquote(response.urljoin(link))
            # リンクのurlが対象としたurlの別ページで抽出されていなかった場合リクエストへ追加
            if self.pagination_check.check(link_url, self.crawl_target_urls, self.logger, self.name):
                urls.add(link_url)

        for url in urls:
            if self.splash_mode:
                req.append(SplashRequest(
                    url=url, callback=self.parse, meta=meta, args=args))
            else:
                req.append(scrapy.Request(url=url, callback=self.parse))
        yield from req

        # クロール時のスパイダーのバージョン情報を記録 ( ex: 'jp_reuters_com_crawl:1.0 / extensions_crawl:1.0' )
        _info = f'{self.name}:{str(self._spider_version)} / {self.EXTENSIONS_CRAWL}:{str(self._extensions_crawl_version)}'

        source_of_information: dict = {}
        for record in self.crawl_urls_list:
            record: dict
            if response.url == record[self.CRAWL_URLS_LIST__LOC]:
                source_of_information[CrawlerResponseModel.SOURCE_OF_INFORMATION__SOURCE_URL] = record[self.CRAWL_URLS_LIST__SOURCE_URL]
                source_of_information[CrawlerResponseModel.SOURCE_OF_INFORMATION__LASTMOD] = record[self.CRAWL_URLS_LIST__LASTMOD]

        yield NewsCrawlItem(
            domain=self.allowed_domains[0],
            url=response.url,
            response_time=datetime.now().astimezone(self.settings['TIMEZONE']),
            response_headers=pickle.dumps(response.headers),
            response_body=pickle.dumps(response.body),
            spider_version_info=_info,
            crawling_start_time=self.news_crawl_input.crawling_start_time,
            source_of_information=source_of_information,
        )

    def closed(self, spider):
        '''spider終了処理'''
        spider_closed(self)

    def _custom_url(self, url: dict) -> str:
        ''' (拡張メソッド)
        requestしたいurlをカスタマイズしたい場合、継承先でオーバーライドして使用する。
        '''
        return url['url']

    def pages_setting(self, default_page_span_from: int, default_page_span_to: int) -> tuple[int, int]:
        ''' (拡張メソッド)
        クロール対象のurlを抽出するページの開始・終了の範囲を決める。
        ・起動時の引数にpagesがある場合は、その指定に従う。
        ・それ以外は、各サイトの標準値に従う。
        '''
        if self.news_crawl_input.page_span_from and self.news_crawl_input.page_span_to:  # ページ範囲指定ありの場合
            self.logger.info(f'=== page_span_from ~ page_span_to {self.news_crawl_input.page_span_from} : {self.news_crawl_input.page_span_to}')
            return self.news_crawl_input.page_span_from, self.news_crawl_input.page_span_to
        else:
            self.logger.info(f'=== page_span_from ~ page_span_to {default_page_span_from} : {default_page_span_to}')
            return default_page_span_from, default_page_span_to
