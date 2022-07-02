import pickle
import scrapy
from typing import Union, Any
from datetime import datetime
from scrapy.spiders import CrawlSpider
from scrapy.http import Response, Request, TextResponse
from urllib.parse import unquote
from news_crawl.items import NewsCrawlItem
from models.mongo_model import MongoModel
from bs4 import BeautifulSoup as bs4
from bs4.element import ResultSet
from scrapy_splash import SplashRequest
from scrapy_splash.response import SplashTextResponse
from news_crawl.spiders.common.spider_init import spider_init
from news_crawl.spiders.common.spider_closed import spider_closed
from news_crawl.spiders.common.lastmod_period_skip_check import LastmodPeriodMinutesSkipCheck
from news_crawl.spiders.common.lastmod_continued_skip_check import LastmodContinuedSkipCheck
from news_crawl.spiders.common.urls_continued_skip_check import UrlsContinuedSkipCheck
from news_crawl.spiders.common.url_pattern_skip_check import url_pattern_skip_check
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

    # 引数の値保存
    kwargs_save: dict                    # 取得した引数を保存
    # MongoDB関連
    mongo: MongoModel                   # MongoDBへの接続を行うインスタンスをspider内に保持。pipelinesで使用。
    # スパイダーの挙動制御関連、固有の情報など
    _crawling_start_time: datetime         # Scrapy起動時点の基準となる時間
    _domain_name = 'sample_com'         # 各種処理で使用するドメイン名の一元管理。継承先で上書き要。

    # 次回クロールポイント情報
    _crawl_point: dict = {}

    # seleniumモード
    selenium_mode: bool = False
    # splashモード
    splash_mode: bool = False

    # 一覧ページの情報を保存 [{'source_url': '', 'lastmod': '', 'loc': ''},,,]
    crawl_urls_list: list[dict[str,Any]] = []

    # クロール対象となったurlリスト[response.url,,,]
    crawl_target_urls: list[str] = []


    # パラメータによる抽出処理のためのクラス
    crawling_continued: LastmodContinuedSkipCheck
    lastmod_period: LastmodPeriodMinutesSkipCheck
    url_continued: UrlsContinuedSkipCheck
    pagination_check: PaginationCheck
    lastmod_continued: LastmodContinuedSkipCheck


    def __init__(self, *args, **kwargs):
        ''' (拡張メソッド)
        親クラスの__init__処理後に追加で初期処理を行う。
        '''
        super().__init__(*args, **kwargs)

        spider_init(self, *args, **kwargs)

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
        #pagination: ResultSet = self.pagination_check(response)
        # if len(pagination) > 0:
        #     self.logger.info(
        #         f"=== parse_news 次のページあり → リクエストに追加 : {pagination[0].get('href')}")
        #     yield scrapy.Request(response.urljoin(pagination[0].get('href')), callback=self.parse_news)

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


        _info = self.name + ':' + str(self._spider_version) + ' / ' \
            + 'extensions_crawl:' + str(self._extensions_crawl_version)

        source_of_information: dict = {}
        for record in self.crawl_urls_list:
            record: dict
            if response.url == record['loc']:
                source_of_information['source_url'] = record['source_url']
                source_of_information['lastmod'] = record['lastmod']

        yield NewsCrawlItem(
            domain=self.allowed_domains[0],
            url=response.url,
            response_time=datetime.now().astimezone(self.settings['TIMEZONE']),
            response_headers=pickle.dumps(response.headers),
            response_body=pickle.dumps(response.body),
            spider_version_info=_info,
            crawling_start_time=self._crawling_start_time,
            source_of_information=source_of_information,
        )

    # def pagination_check(self, response: Response) -> ResultSet:
    #     '''(拡張メソッド)
    #     次ページがあれば、BeautifulSoupのResultSetで返す。
    #     このメソッドは継承先のクラスでオーバーライドして使うことを前提とする。
    #     '''
    #     return ResultSet(response.text, [])

    def closed(self, spider):
        '''spider終了処理'''
        spider_closed(self)

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
            #pages: list = eval(self.kwargs_save['pages'])
            pages:list = list(map(int,str(self.kwargs_save['pages']).split(',')))
            return{'start_page': pages[0], 'end_page': pages[1]}
        else:
            return{'start_page': start_page, 'end_page': end_page}
