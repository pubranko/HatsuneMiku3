import pickle
import scrapy
import re
import scrapy
from datetime import datetime, timedelta
from dateutil import parser
from scrapy.spiders import SitemapSpider
from scrapy.http import Response
from scrapy.utils.sitemap import Sitemap
from news_crawl.items import NewsCrawlItem
from news_crawl.models.mongo_model import MongoModel
from news_crawl.spiders.common.start_request_debug_file_generate import start_request_debug_file_generate
from news_crawl.spiders.common.term_days_Calculation import term_days_Calculation
from news_crawl.spiders.common.spider_init import spider_init
from news_crawl.spiders.common.spider_closed import spider_closed


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

    def start_requests(self):
        for url in self.sitemap_urls:
            yield scrapy.Request(
                url, callback=self._parse_sitemap,  # dont_filter=True
            )

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

    def _custom_url(self, url: dict) -> str:
        ''' (拡張メソッド)
        requestしたいurlをカスタマイズしたい場合、継承先でオーバーライドして使用する。
        '''
        return url['url']
