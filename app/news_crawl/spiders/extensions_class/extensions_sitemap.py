from copy import deepcopy
import pickle
import scrapy
from typing import Union, Any
from datetime import datetime, timedelta
from dateutil import parser
from lxml.etree import _Element
from urllib.parse import unquote
from bs4 import BeautifulSoup as bs4
from scrapy.spiders import SitemapSpider
from scrapy.spiders.sitemap import iterloc
from scrapy.http import Response, Request, TextResponse
from scrapy.utils.sitemap import sitemap_urls_from_robots
from scrapy_selenium import SeleniumRequest
from scrapy_splash import SplashRequest
from scrapy_splash.response import SplashTextResponse
from selenium.webdriver.remote.webdriver import WebDriver
#
from BrownieAtelierMongo.models.mongo_model import MongoModel
from news_crawl.items import NewsCrawlItem
from news_crawl.news_crawl_input import NewsCrawlInput
from news_crawl.spiders.common.lastmod_period_skip_check import LastmodPeriodMinutesSkipCheck
from news_crawl.spiders.common.lastmod_continued_skip_check import LastmodContinuedSkipCheck
from news_crawl.spiders.common.pagination_check import PaginationCheck
from news_crawl.spiders.common.custom_sitemap import CustomSitemap
from news_crawl.spiders.common.start_request_debug_file_generate import start_request_debug_file_generate
from news_crawl.spiders.common.spider_init import spider_init
from news_crawl.spiders.common.spider_closed import spider_closed
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
    # 継承先で上書き要。sitemapindexがある場合、それを指定すること。複数指定不可。
    sitemap_urls: list = ['https://www.sample.com/sitemap.xml', ]
    custom_settings: dict = {
        # 'DEPTH_LIMIT': 2,
        # 'DEPTH_STATS_VERBOSE': True,
    }
    _spider_version: float = 0.0          # spiderのバージョン。継承先で上書き要。
    _extensions_sitemap_version: float = 1.0         # 当クラスのバージョン

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

    # 引数の値保存
    kwargs_save: dict
    # 引数用クラス
    news_crawl_input: NewsCrawlInput
    # 引数による抽出処理のためのクラス
    lastmod_continued: LastmodContinuedSkipCheck
    lastmod_period: LastmodPeriodMinutesSkipCheck
    # ページネーションチェック: 次ページがある場合、そのURLを取得する
    pagination_check: PaginationCheck
    # seleniumモード
    selenium_mode: bool = False
    #sitemap_rules = [(r'.*', 'selenium_parse')]
    # splashモード
    splash_mode: bool = False
    #sitemap_rules = [(r'.*', 'splash_parse')]

    # サイトマップタイプ
    # nomal                 : 通常のscrapyのsitemapでクロールできるタイプ
    # google_news_sitemap   : googleのニュースサイトマップ用にカスタマイズしたタイプ
    # irregular             : イレギラーなサイト用
    sitemap_type = 'nomal'

    # sitemap情報を保存 [{'source_url': response.url, 'lastmod': date_lastmod, 'loc': entry['loc']},,,]
    crawl_urls_list: list[dict[str,Any]] = []

    # クロール対象となったsitemapのurlリスト[response.url,,,]
    crawl_target_urls: list[str] = []

    # 既知のページネーションがある場合、ここにcssセレクター形式で設定する。
    # 注意）href属性の値を取得するように指定すること。
    # 例）'.pagination a[href]::attr(href)'
    known_pagination_css_selectors: list[str] = []
    # ページネーションで追加済みのurlリスト
    pagination_selected_urls: set[str] = set()

    def __init__(self, *args, **kwargs):
        ''' (拡張メソッド)
        親クラスの__init__処理後に追加で初期処理を行う。
        '''
        super().__init__(*args, **kwargs)

        spider_init(self, *args, **kwargs)

        self.pagination_check = PaginationCheck()

    def start_requests(self):
        '''(オーバーライド)
        引数にdirect_crawl_urlsがある場合、sitemapを無視して渡されたurlsをクロールさせる機能を追加。
        また通常版とselenium版の切り替え機能を追加。
        '''
        if 'direct_crawl_urls' in self.kwargs_save:
            for loc in self.kwargs_save['direct_crawl_urls']:
                # しかたなくsitemapから取得したことにして後続を実施
                self.crawl_target_urls.append(loc)
                if self.selenium_mode:
                    yield SeleniumRequest(url=loc, callback=self.selenium_parse, wait_time=2)
                elif self.splash_mode:
                    yield SplashRequest(url=loc, callback=self.parse,
                                        meta={'max_retry_times': 20},
                                        args={
                                            'timeout': 60,  # レンダリングのタイムアウト（秒単位）（デフォルトは30）。
                                            'wait': 2.0,  # ページが読み込まれた後、更新を待機する時間（秒単位）
                                            'resource_timeout': 60.0,  # 個々のネットワーク要求のタイムアウト（秒単位）。
                                            'images': 0,  # 画像はダウンロードしない(0)
                                        })
                else:
                    yield scrapy.Request(url=loc, callback=self.parse)

        else:
            for url in self.sitemap_urls:
                yield scrapy.Request(url=url, callback=self.custom_parse_sitemap)

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
            sitemap = CustomSitemap(
                body, response, self)        # 引数にresponseを追加
            it = self.sitemap_filter(sitemap, response)   # 引数にresponseを追加

            # サイトマップインデックスの場合
            if sitemap.type == 'sitemapindex':
                for loc in iterloc(it, self.sitemap_alternate_links):
                    if any(x.search(loc) for x in self._follow):
                        yield Request(loc, callback=self.custom_parse_sitemap)

            # 子サイトマップの場合
            elif sitemap.type == 'urlset':
                for loc in iterloc(it, self.sitemap_alternate_links):
                    for rule_regex, call_back in self._cbs:
                        if rule_regex.search(loc):
                            # seleniumモードによる切り替え
                            if self.selenium_mode:
                                yield SeleniumRequest(url=loc, callback=call_back, wait_time=2)
                            elif self.splash_mode:
                                yield SplashRequest(url=loc, callback=call_back, meta={'max_retry_times': 20},
                                                    args={
                                    'timeout': 60,  # レンダリングのタイムアウト（秒単位）（デフォルトは30）。
                                    'wait': 1.0,  # ページが読み込まれた後、更新を待機する時間（秒単位）
                                    'resource_timeout': 60.0,  # 個々のネットワーク要求のタイムアウト（秒単位）。
                                    'images': 0,  # 画像はダウンロードしない(0)
                                })
                            else:
                                yield Request(loc, callback=call_back)
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
                # sitemap側と実際のソースは常に一致するわけではなさそう。
                # バッファとして2日間分範囲を広げてチェックする。
                if self.lastmod_period.skip_check(date_lastmod + timedelta(days=2)):
                    crwal_flg = False
                if self.lastmod_continued.skip_check(date_lastmod):
                    crwal_flg = False
            else:
                if url_pattern_skip_check(entry['loc'], self.kwargs_save):
                    crwal_flg = False
                if self.lastmod_period.skip_check(date_lastmod):
                    crwal_flg = False
                if self.lastmod_continued.skip_check(date_lastmod):
                    crwal_flg = False

            # クロール対象となった場合
            if crwal_flg:
                # カスタムurlの指定がある場合url変換する。
                if self._custom_url_flg:
                    entry['loc'] = self._custom_url(entry)
                # 子サイトマップ以外
                if not entries.type == 'sitemapindex':
                    # クロールurl情報を保存
                    self.crawl_urls_list.append({
                        'source_url': response.url,
                        'lastmod': date_lastmod,
                        'loc': entry['loc']})
                    # クロール対象となったurlを保存
                    self.crawl_target_urls.append(entry['loc'])
                yield entry

        # 単一のサイトマップからクロールする場合、そのページの最大更新時間、
        # サイトマップインデックスをクロールする場合、その最大更新時間をドメイン単位の最大更新時間とする。
        # ただし、クロールするlastmodの範囲指定でTOが指定されている場合、その時間を最大更新時間とする。
        if not self.domain_lastmod:
            if type(self.lastmod_period.lastmod_period_minutes_to) == datetime:
                self.domain_lastmod = self.lastmod_period.lastmod_period_minutes_to
            else:
                self.domain_lastmod = self.lastmod_continued.max_lastmod_dicision(
                    parser.parse(max_lstmod).astimezone(self.settings['TIMEZONE']))
            # クロールポイントを更新する。
            self._crawl_point = {
                'latest_lastmod': self.domain_lastmod,
                'crawling_start_time': self._crawling_start_time,
            }

    def parse(self, response: TextResponse):
        '''
        取得したレスポンスよりDBへ書き込み
        '''
        # selenium、splash、通常モードにより処理を切り分ける
        meta = {}
        args = {}

        if self.splash_mode:
            meta = {'max_retry_times': 20}
            args = {
                'timeout': 60,  # レンダリングのタイムアウト（秒単位）（デフォルトは30）。
                'wait': 2.0,  # ページが読み込まれた後、更新を待機する時間（秒単位）
                'resource_timeout': 60.0,  # 個々のネットワーク要求のタイムアウト（秒単位）。
                'images': 0,  # 画像はダウンロードしない(0)
            }

        # 既知のページネーションページ内の対象urlを抽出
        urls: set = set()
        req: list = []
        for css_selector in self.known_pagination_css_selectors:
            # 既知のページネーションのurlの場合リクエストへ追加
            links = response.css(f'{css_selector}::attr(href)').getall()
            for link in links:
                # 相対パスの場合絶対パスへ変換。また%エスケープされたものはUTF-8へ変換
                url: str = unquote(response.urljoin(link))
                # クロール対象となったurlを保存
                self.pagination_selected_urls.add(url)
                self.logger.info(f'=== {self.name} 既知ページネーション : {url}')
                urls.add(url)

        # ページ内の全リンクを抽出（重複分はsetで削除）
        for link in set(response.css('[href]::attr(href)').getall()):
            # 相対パスの場合絶対パスへ変換。また%エスケープされたものはUTF-8へ変換
            link_url: str = unquote(response.urljoin(link))
            # リンクのurlがsitemapで対象としたurlの別ページ、かつ、既知のページネーションで
            # 抽出されていなかった場合リクエストへ追加
            if self.pagination_check.check(link_url, self.crawl_target_urls, self.logger, self.name):
                urls.add(link_url)

        for url in urls:
            if self.splash_mode:
                req.append(SplashRequest(
                    url=url, callback=self.parse, meta=meta, args=args))
            else:
                req.append(scrapy.Request(url=url, callback=self.parse))
        yield from req

        ###
        _info = self.name + ':' + str(self._spider_version) + ' / ' \
            + 'extensions_sitemap:' + str(self._extensions_sitemap_version)

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

    def selenium_parse(self, response: TextResponse):
        '''
        取得したレスポンスよりDBへ書き込み
        '''
        any: Any = response.request
        driver: WebDriver = any.meta['driver']

        driver.set_page_load_timeout(60)
        # driver.implicitly_wait(15)
        driver.set_script_timeout(60)

        # ページ内の全リンクを抽出（重複分はsetで削除）
        # driverから直接リンク要素を取得しても、DOMで参照中に変わってしまうことが発生した。
        # そのためpage_sourceをもとに一度bs4でparseしてDOMの影響を受けないように対応を行った。
        #   unknown_links = set([unquote(el.get_attribute("href")) for el in driver.find_elements_by_css_selector('[href]')])
        soup: bs4 = bs4(driver.page_source, 'lxml')
        _ = soup.select('[href]')
        unknown_links = [a['href'] for a in _]

        # 既知のページネーションページ内の対象urlを抽出
        urls: set = set()
        req: list = []
        for css_selector in self.known_pagination_css_selectors:
            # 既知のページネーションのurlの場合リクエストへ追加
            elem = driver.find_elements_by_css_selector(css_selector)
            known_links = [unquote(el.get_attribute("href")) for el in elem]

            for link in known_links:
                # 相対パスの場合絶対パスへ変換。また%エスケープされたものはUTF-8へ変換
                url: str = unquote(response.urljoin(link))
                self.pagination_selected_urls.add(url)
                self.logger.info(f'=== {self.name} 既知ページネーション : {url}')
                urls.add(url)

        for link in unknown_links:
            # 相対パスの場合絶対パスへ変換。また%エスケープされたものはUTF-8へ変換
            link_url: str = unquote(response.urljoin(link))
            # リンクのurlがsitemapで対象としたurlの別ページ、かつ、既知のページネーションで
            # 抽出されていなかった場合リクエストへ追加
            if self.pagination_check.check(link_url, self.crawl_target_urls, self.logger, self.name):
                urls.add(link_url)

        for url in urls:
            req.append(SeleniumRequest(
                url=url, callback=self.selenium_parse, wait_time=2))
        yield from req
        ###

        _info = self.name + ':' + str(self._spider_version) + ' / ' \
            + 'extensions_sitemap:' + str(self._extensions_sitemap_version)

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
            response_body=pickle.dumps(driver.page_source),
            spider_version_info=_info,
            crawling_start_time=self._crawling_start_time,
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

    # @classmethod
    # def irregular_sitemap_parse(cls, d: dict, el: _Element, name: Any):
    def irregular_sitemap_parse(self, d: dict, el: _Element, name: Any):
        '''
        イレギラーなサイトマップの場合、独自のxml解析を行う。
        各スパイダーでオーバーライドして使用する。
        '''
        return d

    # def temp_pagination_check(self, link_url: str) -> bool:
    #     ''' '''
    #     check_flg: bool = False  # ページネーションのリンクの場合、Trueとする。
    #     # チェック対象のurlを解析
    #     link_parse: ParseResult = urlparse(link_url)
    #     # 解析したクエリーをdictへ変換 page=2&a=1&b=2 -> {'page': ['2'], 'a': ['1'], 'b': ['2']}
    #     link_query: dict = parse_qs(link_parse.query)

    #     # 追加リクエスト済み情報の準備
    #     pagination_selected_pathes: set = set()
    #     pagination_selected_same_path_queries: list = []
    #     for pagination_selected_url in self.pagination_selected_urls:
    #         _ = urlparse(pagination_selected_url)
    #         pagination_selected_pathes.add(_.path)
    #         if link_parse.path == _.path:
    #             pagination_selected_same_path_queries.append(parse_qs(_.query))

    #     # sitemapから取得したurlより順にチェック
    #     for _ in self.crawl_target_urls:
    #         # sitemapから取得したurlを解析
    #         crawl_target_parse: ParseResult = urlparse(_)
    #         # netloc（hostnameだけでなくportも含む）が一致すること
    #         if crawl_target_parse.netloc == link_parse.netloc:
    #             # まだ同一ページの追加リクエストされていない場合（path部分で判定）
    #             if not link_parse.path in pagination_selected_pathes:
    #                 # パスの末尾にページが付与されているケースの場合、追加リクエストの対象とする。
    #                 # 例）https://www.sankei.com/article/20210321-VW5B7JJG7JKCBG5J6REEW6ZTBM/
    #                 #     https://www.sankei.com/article/20210321-VW5B7JJG7JKCBG5J6REEW6ZTBM/2/
    #                 _ = re.compile(r'/[0-9]{1,3}/*$')
    #                 if re.search(_, link_parse.path):
    #                     # pathの末尾のページ情報を削除
    #                     # 例）〜OYT1T50226/2/ -> 〜OYT1T50226
    #                     link_type1 = _.sub('', link_parse.path)
    #                     # 末尾のスラッシュがあれば削除
    #                     _ = re.compile(r'/$')
    #                     crawl_type1 = _.sub('', crawl_target_parse.path)
    #                     # ページ情報部を除いて比較し一致した場合
    #                     if crawl_type1 == link_type1:
    #                         self.logger.info(
    #                             f'=== {self.name} ページネーションtype1 : {link_url}')
    #                         check_flg = True

    #                 # 拡張子除去後の末尾にページが付与されているケースの場合、追加リクエストの対象とする。
    #                 # 例）https://www.sankei.com/politics/news/210521/plt2105210030-n1.html
    #                 #     https://www.sankei.com/politics/news/210521/plt2105210030-n2.html
    #                 _ = re.compile(r'[^0-9][0-9]{1,3}\.(html|htm)$')
    #                 if re.search(_, link_parse.path):
    #                     # 例）〜n1.html -> 〜n
    #                     link_type2 = _.sub('', link_parse.path)
    #                     crawl_type2 = _.sub('', crawl_target_parse.path)
    #                     # 末尾の拡張子やページ情報を除いて比較し一致した場合
    #                     if crawl_type2 == link_type2:
    #                         self.logger.info(
    #                             f'=== {self.name} ページネーションtype2 : {link_url}')
    #                         check_flg = True

    #             # クエリーにページが付与されているケースの場合、追加リクエストの対象とする。
    #             # ただし、以下の場合は対象外。
    #             # ・既に同一ページの追加リクエスト済みの場合。
    #             # ・１ページ目の場合。※sitemap側でリクエスト済みのため。
    #             # 例）https://webronza.asahi.com/national/articles/2022042000004.html
    #             #     https://webronza.asahi.com/national/articles/2022042000004.html?a=b&c=d
    #             #     https://webronza.asahi.com/national/articles/2022042000004.html?page=1&a=b&e=f
    #             #     https://webronza.asahi.com/national/articles/2022042000004.html?page=1&m=n&g=h
    #             #     https://webronza.asahi.com/national/articles/2022042000004.html?page=2&a=b&e=f
    #             #     https://webronza.asahi.com/national/articles/2022042000004.html?page=2&m=n&g=h
    #             if crawl_target_parse.path == link_parse.path:
    #                 # リンクのクエリーにページ指定と思われるkeyの存在チェック （複数該当することは無いことを祈る、、、）
    #                 page_keys = ['page', 'pagination', 'pager', 'p']
    #                 link_query_selected_items: list[tuple] = []
    #                 for link_query_key, link_query_value in link_query.items():
    #                     if link_query_key in page_keys:
    #                         link_query_selected_items.append(
    #                             (link_query_key, link_query_value))

    #                 # linkにpege系クエリーがあった場合、
    #                 for link_query_selected_item in link_query_selected_items:
    #                     check_flg = True
    #                     for same_path_query in pagination_selected_same_path_queries:
    #                         # keyが一致
    #                         if link_query_selected_item[0] in same_path_query:
    #                             # valueが一致(同一ページ)した場合は対象外
    #                             if link_query_selected_item[1][0] == same_path_query[link_query_selected_item[0]][0]:
    #                                 check_flg = False
    #                             # page=1は対象外
    #                             elif link_query_selected_item[1][0] == str(1):
    #                                 check_flg = False
    #                     if check_flg:
    #                         self.logger.info(
    #                             f'=== {self.name} ページネーションtype3 : {link_url}')

    #     # クロール対象となったurlを保存
    #     if check_flg:
    #         self.pagination_selected_urls.add(link_url)

    #     return check_flg
