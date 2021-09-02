import pickle
import scrapy
import re
import scrapy
from datetime import datetime, timedelta
from dateutil import parser
from scrapy.spiders import SitemapSpider
from scrapy.http import Response
from scrapy.utils.sitemap import Sitemap
from scrapy.spiders import Rule
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders.sitemap import iterloc
from scrapy.http import Request
from scrapy.http import Response
from scrapy.utils.sitemap import Sitemap, sitemap_urls_from_robots
from scrapy_selenium import SeleniumRequest
from selenium.webdriver.remote.webdriver import WebDriver
from news_crawl.items import NewsCrawlItem
from models.mongo_model import MongoModel
from news_crawl.spiders.common.start_request_debug_file_generate import start_request_debug_file_generate
from news_crawl.spiders.common.term_days_Calculation import term_days_Calculation
from news_crawl.spiders.common.spider_init import spider_init
from news_crawl.spiders.common.spider_closed import spider_closed
from news_crawl.spiders.common.lastmod_period_skip_check import LastmodPeriodMinutesSkipCheck
from news_crawl.spiders.common.crawling_continued_skip_check import CrawlingContinuedSkipCheck
from news_crawl.spiders.common.url_pattern_skip_check import url_pattern_skip_check


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
    _crawling_start_time: datetime         # Scrapy起動時点の基準となる時間
    _domain_name = 'sample_com'         # 各種処理で使用するドメイン名の一元管理。継承先で上書き要。

    # 次回クロールポイント情報
    _crawl_point: dict = {}
    # sitemap_urlsに複数のサイトマップを指定した場合、その数だけsitemap_filterが可動する。その際、どのサイトマップか判別できるように処理中のサイトマップと連動するカウント。
    _sitemap_urls_count: int = 0
    # sitemapのリンク先urlをカスタマイズしたい場合、継承先のクラスでTrueにする。
    # Trueの場合、継承先でオーバーライドしたcustom_url()メソッドを使い、urlをカスタムする。
    _custom_url_flg: bool = False
    # seleniumモード
    selenium_mode: bool = False
    rules = (Rule(LinkExtractor(allow=(r'.+')), callback='parse'),)

    def __init__(self, *args, **kwargs):
        ''' (拡張メソッド)
        親クラスの__init__処理後に追加で初期処理を行う。
        '''
        super().__init__(*args, **kwargs)

        spider_init(self, *args, **kwargs)

    def start_requests(self):
        '''(オーバーライド)
        通常版とselenium版の切り替え機能を追加。
        '''
        for url in self.sitemap_urls:
            yield scrapy.Request(url, self.custom_parse_sitemap)

    def custom_parse_sitemap(self, response):
        '''selenium版のparse_sitemap'''
        if response.url.endswith('/robots.txt'):
            for url in sitemap_urls_from_robots(response.text, base_url=response.url):
                yield Request(url, callback=self.custom_parse_sitemap)
        else:
            body = self._get_sitemap_body(response)
            if body is None:
                self.logger.warning("Ignoring invalid sitemap: %(response)s",
                                    {'response': response}, extra={'spider': self})
                return

            s = Sitemap(body)
            it = self.sitemap_filter(s)

            # 親サイトマップの場合
            if s.type == 'sitemapindex':
                for loc in iterloc(it, self.sitemap_alternate_links):
                    if any(x.search(loc) for x in self._follow):
                        yield Request(loc, callback=self.custom_parse_sitemap)

            # 子サイトマップの場合
            elif s.type == 'urlset':
                for loc in iterloc(it, self.sitemap_alternate_links):
                    for r, c in self._cbs:
                        if r.search(loc):
                            # seleniumモードによる切り替え
                            if self.selenium_mode:
                                yield SeleniumRequest(url=loc, callback=c)
                            else:
                                yield Request(loc, callback=c)
                            break

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

        # チェック用クラスの初期化
        lastmod_pefiod = LastmodPeriodMinutesSkipCheck(self,self._crawling_start_time,self.kwargs_save)
        crawling_continued = CrawlingContinuedSkipCheck(self._crawl_point,sitemap_url,self.kwargs_save)

        # 処理中のサイトマップ内で、最大のlastmodとurlを記録するエリア
        max_lstmod: str = ''
        max_url: str = ''

        for entry in entries:
            '''
            引数に絞り込み指定がある場合、その条件を満たす場合のみ対象とする。
            '''
            entry: dict
            if max_lstmod < entry['lastmod']:
                max_lstmod = entry['lastmod']
                max_url = entry['loc']
            crwal_flg: bool = True
            date_lastmod = parser.parse(entry['lastmod']).astimezone(
                self.settings['TIMEZONE'])

            if  url_pattern_skip_check(entry['loc'],self.kwargs_save):
                crwal_flg = False
            if lastmod_pefiod.skip_check(date_lastmod):    # lastmod絞り込み範囲指定あり
                crwal_flg = False
            if crawling_continued.skip_check(date_lastmod,entry['loc']):
                crwal_flg = False

            if crwal_flg:
                if self._custom_url_flg:
                    entry['loc'] = self._custom_url(entry)
                yield entry

        # サイトマップごとの最大更新時間を記録(controllerコレクションへ保存する内容)
        _ = parser.parse(max_lstmod)

        self._crawl_point[sitemap_url] = {
            'latest_lastmod': _,
            'latest_url': max_url,
            'crawling_start_time': self._crawling_start_time,
        }
        self._sitemap_urls_count += 1  # 次のサイトマップurl用にカウントアップ

    def parse(self, response: Response):
        '''
        取得したレスポンスよりDBへ書き込み
        '''
        _info = self.name + ':' + str(self._spider_version) + ' / ' \
            + 'extensions_sitemap:' + str(self._extensions_sitemap_version)

        yield NewsCrawlItem(
            domain=self.allowed_domains[0],
            url=response.url,
            response_time=datetime.now().astimezone(self.settings['TIMEZONE']),
            response_headers=pickle.dumps(response.headers),
            response_body=pickle.dumps(response.body),
            spider_version_info=_info,
            crawling_start_time=self._crawling_start_time,
        )

    def selenium_parse(self, response: Response):
        '''
        seleniumu版parse。JavaScript処理終了後のレスポンスよりDBへ書き込み
        '''
        driver: WebDriver = response.request.meta['driver']
        # Javascript実行が終了するまで最大30秒間待つように指定
        driver.set_script_timeout(30)

        _info = self.name + ':' + str(self._spider_version) + ' / ' \
            + 'extensions_sitemap:' + str(self._extensions_sitemap_version)

        yield NewsCrawlItem(
            domain=self.allowed_domains[0],
            url=response.url,
            response_time=datetime.now().astimezone(self.settings['TIMEZONE']),
            response_headers=pickle.dumps(response.headers),
            response_body=pickle.dumps(driver.page_source),
            spider_version_info=_info,
            crawling_start_time=self._crawling_start_time,
        )

    def closed(self, spider):
        '''spider終了処理'''
        spider_closed(self)

    def _custom_url(self, url: dict) -> str:
        ''' (拡張メソッド)
        requestしたいurlをカスタマイズしたい場合、継承先でオーバーライドして使用する。
        '''
        return url['url']
