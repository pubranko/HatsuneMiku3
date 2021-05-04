from typing import Any
from scrapy.spiders import SitemapSpider
from datetime import datetime
import sys
import pickle
from news_crawl.items import NewsCrawlItem
from news_crawl.models.mongo_model import MongoModel
from news_crawl.models.crawler_controller_model import CrawlerControllerModel
from datetime import datetime, timedelta
from dateutil import parser


class ExtensionsSitemapSpider(SitemapSpider):
    '''
    SitemapSpiderの機能を拡張したクラス。
    Override → __init__(),parse(),closed()
    (前提)
    name, allowed_domains, sitemap_urls, _domain_name, spider_versionの値は当クラスを継承するクラスで設定すること。
    sitemap_filter()メソッドのオーバーライドも継承先のクラスで行うこと。
    '''
    name: str = 'extension_sitemap'                                 # 継承先で上書き要。
    allowed_domains: list = ['sample.com']                          # 継承先で上書き要。
    sitemap_urls: list = ['https://www.sample.com/sitemap.xml', ]   # 継承先で上書き要。
    custom_settings: dict = {
        'DEPTH_LIMIT': 2,
        'DEPTH_STATS_VERBOSE': True
    }
    spider_version: float = 0.0          # spiderのバージョン。継承先で上書き要。
    # 引数の値保存
    _lastmod_recent_time: int = 0       # 直近の15分など。分単位で指定することにしよう。0は制限なしとする。
    _url_pattern: str = ''              # 指定したパターンをurlに含むもので限定(正規表現)
    _continued: bool = False            # 前回の続きから(最後に取得したlastmodの日時)
    _term_days: int = 0                 # 当日を含め、指定した日数を含むurlに限定
    _debug_flg = False                  # debugモードを引数で指定された場合True
    # MongoDB関連
    mongo: MongoModel                   # MongoDBへの接続を行うインスタンスをspider内に保持。pipelinesで使用。
    _crawler_controller_recode: Any     # crawler_controllerコレクションから取得した対象ドメインのレコード
    # スパイダーの挙動制御関連、固有の情報など
    _until_this_time: datetime          # sitemapで対象とするlastmodに制限をかける時間
    _crawl_start_time: datetime         # Scrapy起動時点の基準となる時間
    _crawl_start_time_iso: str          # Scrapy起動時点の基準となる時間(ISOフォーマットの文字列)
    # sitemapで最初の1件目にあるlastmod(最新の更新)を保持。pipelineで使用。
    _domain_name = 'sample_com'         # 各種処理で使用するドメイン名の一元管理。継承先で上書き要。
    _latest_lastmod: str
    _latest_url: str                    # 上記'latest_lastmod'のurl
    _spider_type: str = 'SitemapSpider'  # spiderの種類。pipelineで使用。
    _extensions_sitemap_version: float = 1.0         # 当クラスのバージョン

    # sitemap_urlsに複数のサイトマップを指定した場合、その数だけsitemap_filterが可動する。その際、どのサイトマップか判別できるように処理中のサイトマップと連動するカウント。
    _sitemap_urls_count: int = 0
    # crawler_controllerコレクションへ書き込むレコードのdomain以降のレイアウト雛形。※最上位のKeyのdomainはサイトの特性にかかわらず固定とするため。
    _sitemap_contened: dict = {name: {},}

    def __init__(self, *args, **kwargs):
        super(ExtensionsSitemapSpider, self).__init__(*args, **kwargs)
        self.mongo = MongoModel()
        # 引数チェック・保存
        # 単項目チェック
        if 'debug' in kwargs:
            if kwargs['debug'] == 'Yes':
                self._debug_flg = True
                print('=== debugモード ON ===')
                _file = open('debug_entries.txt', 'w')  # 初期化
                _file.close()

        if 'term_days' in kwargs:
            if kwargs['term_days'].isdecimal():
                self._term_days = int(kwargs['term_days'])
            else:
                sys.exit('引数エラー：term_daysは数字(0〜9)のみ使用可。日単位で指定してください。')
        if 'url_pattern' in kwargs:
            self._url_pattern = kwargs['url_pattern']
        if 'lastmod_recent_time' in kwargs:
            if kwargs['lastmod_recent_time'].isdecimal():
                self._lastmod_recent_time = int(kwargs['lastmod_recent_time'])
            else:
                sys.exit('引数エラー：lastmod_recent_timeは数字(0〜9)のみ使用可。分単位で指定してください。')
        if 'continued' in kwargs:
            if kwargs['continued'] == 'Yes':
                self.crawler_controller = CrawlerControllerModel(self.mongo)
                self._crawler_controller_recode = self.crawler_controller.find_one(
                    {'domain': self._domain_name})
                if self._crawler_controller_recode == None:
                    sys.exit('引数エラー：domain = ' + self._domain_name +
                             ' は前回のcrawl情報がありません。初回から"continued"の使用は不可です。')
                else:
                    self._continued = True
                    #self._sitemap_contened: dict = { self.name : '', }
            else:
                sys.exit('引数エラー：continuedに使用できるのは、"Yes"のみです。')
        # 関連チェック
        if 'lastmod_recent_time' in kwargs and 'continued' in kwargs:
            sys.exit('引数エラー：lastmod_recent_timeとcontinuedは同時には使えません。')

        print('=== ExtensionsSitemapSpider の __init__終了')

    def parse(self, response):
        '''
        取得したレスポンスよりDBへ書き込み
        '''
        _info = self.name + ':' + str(self.spider_version) + ' / ' \
            + 'extensions_sitemap:' + str(self._extensions_sitemap_version)

        yield NewsCrawlItem(
            url=response.url,
            response_time=datetime.now().astimezone(self.settings['TIMEZONE']),
            response_headers=pickle.dumps(response.headers),
            response_body=pickle.dumps(response.body),
            spider_version_info=_info
        )  # ログに不要な値を出さないようitems.pyで細工して文字列を短縮。

    def closed(self, spider):
        '''
        spider終了時、次回クロール向けの情報をcrawler_controllerへ記録する。
        '''
        self.logger.info('=== Spider closed: %s', self.name)
        # SitemapSpiderの場合、sitemapでどこまで見たか記録する。
        if self._spider_type == 'SitemapSpider':
            crawler_controller = CrawlerControllerModel(self.mongo)
            crawler_controller.update(
                {'domain': self._domain_name},
                {'domain': self._domain_name,
                self.name:self._sitemap_contened[self.name],
                },
            )

        self.mongo.close()

    def sitemap_filter_common_prosses(self, entries):
        ''' 独自に拡張。
        １．起動時の引数に応じた処理を行うため、以下の抽出に使う変数を求める。
          => self._crawl_start_time, self._crawl_start_time_iso, self._until_this_time, self._tarm_days_list, self._until_this_time
        ２．デバックモードが指定された場合、entriesをdebug_entries.txtへ出力する。
        '''

        # sitemap調査用。debugモードの場合のみ。
        if self._debug_flg:
            _file = open('debug_entries.txt', 'a')
            for _entry in entries:
                _file.write(_entry['lastmod'] + ', ' + _entry['loc'] + '\n')
            _file.close()

        # クロール開始時間
        self._crawl_start_time = datetime.now().astimezone(
            self.settings['TIMEZONE'])
        self._crawl_start_time_iso = self._crawl_start_time.isoformat()

        # 直近の数分間の指定がある場合
        if self._lastmod_recent_time != 0:
            self._until_this_time = self._crawl_start_time - \
                timedelta(minutes=self._lastmod_recent_time)
        # 直近の数日間の指定がある場合
        if self._term_days != 0:
            self._tarm_days_list: list = [
                (self._crawl_start_time - timedelta(days=i)).strftime('%y%m%d') for i in range(self._term_days)]
        # 前回からの続きの指定がある場合
        if self._continued:
            self._until_this_time = parser.parse(
                self._crawler_controller_recode[self.name][self.sitemap_urls[self._sitemap_urls_count]]['latest_lastmod'])
