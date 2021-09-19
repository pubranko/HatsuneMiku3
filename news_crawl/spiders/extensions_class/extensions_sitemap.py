import pickle
from pandas.core import series
import scrapy
from typing import Union, Any
from datetime import datetime
from dateutil import parser
from lxml.etree import _Element
import pandas as pd
from pandas import DataFrame,Series
import numpy

from scrapy.spiders import SitemapSpider, Rule
from scrapy.spiders.sitemap import iterloc
from scrapy.linkextractors import LinkExtractor
from scrapy.http import Response, Request
from scrapy.utils.sitemap import sitemap_urls_from_robots, Sitemap
from scrapy_selenium import SeleniumRequest
from selenium.webdriver.remote.webdriver import WebDriver
from news_crawl.items import NewsCrawlItem
from models.mongo_model import MongoModel
from news_crawl.spiders.common.start_request_debug_file_generate import start_request_debug_file_generate
from news_crawl.spiders.common.spider_init import spider_init
from news_crawl.spiders.common.spider_closed import spider_closed
from news_crawl.spiders.common.lastmod_period_skip_check import LastmodPeriodMinutesSkipCheck
from news_crawl.spiders.common.crawling_continued_skip_check import CrawlingContinuedSkipCheck
from news_crawl.spiders.common.url_pattern_skip_check import url_pattern_skip_check
from news_crawl.spiders.common.custom_sitemap import CustomSitemap

class ExtensionsSitemapSpider(SitemapSpider):
    '''
    SitemapSpiderの機能を拡張したクラス。
    (前提)
    name, allowed_domains, sitemap_urls, _domain_name, spider_versionの値は当クラスを継承するクラスで設定すること。
    sitemap_filter()メソッドのオーバーライドも継承先のクラスで行うこと。
    '''
    name: str = 'extension_sitemap'                                 # 継承先で上書き要。
    allowed_domains: list = ['sample.com']                          # 継承先で上書き要。
    # 継承先で上書き要。sitemapindexがある場合、それを指定すること。複数指定不可。
    sitemap_urls: list = ['https://www.sample.com/sitemap.xml', ]
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

    # ＜domain_lastmodについて＞
    # 複数のsitemapを読み込む場合、最大のlastmodは以下のように判断する。
    # 1. sitemap_indexがない場合 → そのページの最大lastmod
    # 2. sitemap_indexから複数のsitemapを読み込んだ場合 → sitemap_indexの最大lastmod
    # ※1.2.について → 順番にsitemapを呼び出す際、タイムラグによる取りこぼしがないようにするため。
    # 3. ただし、クロールするlastmodの範囲指定(lastmod_period_minutes)でTOが指定されている場合、その時間を最大更新時間とする。(テストで利用しやすくするため)
    domain_lastmod: Union[datetime, None] = None

    # パラメータによる抽出処理のためのクラス
    crawling_continued: CrawlingContinuedSkipCheck
    lastmod_pefiod: LastmodPeriodMinutesSkipCheck
    # seleniumモード
    selenium_mode: bool = False
    rules = (Rule(LinkExtractor(allow=(r'.+')), callback='parse'),)
    # sitemap_index用のルール
    #rules = (Rule(LinkExtractor(allow=(r'.+/sitemap.xml$')), callback='sitemap_index_parse'),)

    # イレギラーなサイトマップの場合、Trueにしてxml解析を各スパイダー用に切り替える。
    irregular_sitemap_parse_flg: bool = False

    # sitemap情報を保存
    sitemap_records: list = []

    def __init__(self, *args, **kwargs):
        ''' (拡張メソッド)
        親クラスの__init__処理後に追加で初期処理を行う。
        '''
        super().__init__(*args, **kwargs)

        spider_init(self, *args, **kwargs)

    def start_requests(self):
        '''(オーバーライド)
        引数にdirect_crawl_urlsがある場合、sitemapを無視して渡されたurlsをクロールさせる機能を追加。
        また通常版とselenium版の切り替え機能を追加。
        '''
        if 'direct_crawl_urls' in self.kwargs_save:
            for loc in self.kwargs_save['direct_crawl_urls']:
                for r, c in self._cbs:
                    if r.search(loc):
                        # seleniumモードによる切り替え
                        if self.selenium_mode:
                            yield SeleniumRequest(url=loc, callback=c)
                        else:
                            yield Request(loc, callback=c)
                        break
        else:
            for url in self.sitemap_urls:
                yield scrapy.Request(url, self.custom_parse_sitemap)

    def custom_parse_sitemap(self, response: Response):
        '''
        カスタマイズ版の_parse_sitemap
        通常版とselenium版の切り替え機能を追加。
        '''
        if response.url.endswith('/robots.txt'):
            for url in sitemap_urls_from_robots(response.text, base_url=response.url):
                yield Request(url, callback=self.custom_parse_sitemap)
        else:
            body = self._get_sitemap_body(response)
            if body is None:
                self.logger.warning("Ignoring invalid sitemap: %(response)s",
                                    {'response': response}, extra={'spider': self})
                return

            #s = Sitemap(body)
            s = CustomSitemap(body, response, self)        # 引数にresponseを追加
            it = self.sitemap_filter(s, response)   # 引数にresponseを追加

            # サイトマップインデックスの場合
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

    def sitemap_filter(self, entries: CustomSitemap, response: Response):
        '''
        親クラスのSitemapSpiderの同名メソッドをオーバーライド。
        entriesには、サイトマップから取得した情報(loc,lastmodなど）が辞書形式で格納されている。
        例)entryの使い方：entry['lastmod'],entry['loc'],entry.items()
        サイトマップから取得したurlへrequestを行う前に条件で除外することができる。
        対象のurlの場合、”yield entry”で返すと親クラス側でrequestされる。
        '''
        # ExtensionsSitemapSpiderクラスを継承した場合のsitemap_filter共通処理
        start_request_debug_file_generate(
            self.name, response.url, entries, self.kwargs_save)

        # 処理中のサイトマップ内で、最大のlastmodを記録するエリア
        max_lstmod: str = ''

        for entry in entries:
            '''
            引数に絞り込み指定がある場合、その条件を満たす場合のみ対象とする。
            '''
            entry: dict
            if max_lstmod < entry['lastmod']:
                max_lstmod = entry['lastmod']
            crwal_flg: bool = True
            date_lastmod = parser.parse(entry['lastmod']).astimezone(
                self.settings['TIMEZONE'])

            if entries.type == 'sitemapindex':
                if self.lastmod_pefiod.skip_check(date_lastmod):
                    crwal_flg = False
                if self.crawling_continued.skip_check(date_lastmod):
                    crwal_flg = False
            else:
                if url_pattern_skip_check(entry['loc'], self.kwargs_save):
                    crwal_flg = False
                if self.lastmod_pefiod.skip_check(date_lastmod):
                    crwal_flg = False
                if self.crawling_continued.skip_check(date_lastmod):
                    crwal_flg = False

            if crwal_flg:
                if self._custom_url_flg:
                    entry['loc'] = self._custom_url(entry)

                self.sitemap_records.append({
                    'sitemap_url':response.url,
                    'lastmod':date_lastmod,
                    'loc':entry['loc']})
                yield entry

        # 単一のサイトマップからクロールする場合、そのページの最大更新時間、
        # サイトマップインデックスをクロールする場合、その最大更新時間をドメイン単位の最大更新時間とする。
        # ただし、クロールするlastmodの範囲指定でTOが指定されている場合、その時間を最大更新時間とする。
        if not self.domain_lastmod:
            if type(self.lastmod_pefiod.lastmod_period_minutes_to) == datetime:
                self.domain_lastmod = self.lastmod_pefiod.lastmod_period_minutes_to
            else:
                self.domain_lastmod = self.crawling_continued.max_lastmod_dicision(
                    parser.parse(max_lstmod).astimezone(self.settings['TIMEZONE']))
            # クロールポイントを更新する。
            self._crawl_point = {
                'latest_lastmod': self.domain_lastmod,
                'crawling_start_time': self._crawling_start_time,
            }

    def parse(self, response: Response):
        '''
        取得したレスポンスよりDBへ書き込み
        '''
        _info = self.name + ':' + str(self._spider_version) + ' / ' \
            + 'extensions_sitemap:' + str(self._extensions_sitemap_version)

        sitemap_data:dict = {}
        for record in self.sitemap_records:
            record:dict
            if response.url == record['loc']:
                sitemap_data['sitemap_url'] = record['sitemap_url']
                sitemap_data['lastmod'] = record['lastmod']

        yield NewsCrawlItem(
            domain=self.allowed_domains[0],
            url=response.url,
            response_time=datetime.now().astimezone(self.settings['TIMEZONE']),
            response_headers=pickle.dumps(response.headers),
            response_body=pickle.dumps(response.body),
            spider_version_info=_info,
            crawling_start_time=self._crawling_start_time,
            sitemap_data=sitemap_data,
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

        sitemap_data:list = []
        for rec in self.sitemap_records:
            if response.url in rec:
                sitemap_data = rec
                sitemap_data.remove(response.url)

        yield NewsCrawlItem(
            domain=self.allowed_domains[0],
            url=response.url,
            response_time=datetime.now().astimezone(self.settings['TIMEZONE']),
            response_headers=pickle.dumps(response.headers),
            response_body=pickle.dumps(driver.page_source),
            spider_version_info=_info,
            crawling_start_time=self._crawling_start_time,
            sitemap_data=sitemap_data,
        )

    def closed(self, spider):
        '''spider終了処理'''
        spider_closed(self)

    def _custom_url(self, url: dict) -> str:
        ''' (拡張メソッド)
        requestしたいurlをカスタマイズしたい場合、継承先でオーバーライドして使用する。
        '''
        return url['url']

    @classmethod
    def irregular_sitemap_parse(cls, d: dict, el: _Element, name: Any):
        '''
        イレギラーなサイトマップの場合、独自のxml解析を行う。
        各スパイダーでオーバーライドして使用する。
        '''
        return d
