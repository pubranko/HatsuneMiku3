from typing import Any, Tuple
from scrapy.spiders import SitemapSpider
from datetime import datetime
import pickle
import sys
from news_crawl.items import NewsCrawlItem
from news_crawl.models.mongo_model import MongoModel
from news_crawl.models.crawler_controller_model import CrawlerControllerModel
from datetime import datetime, timedelta
from dateutil import parser
from news_crawl.settings import TIMEZONE
from news_crawl.spiders.function import extensions_sitemap_argument_check


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
    _extensions_sitemap_version: float = 1.0         # 当クラスのバージョン

    # 引数の値保存
    # _lastmod_recent_time: int = 0       # 直近の15分など。分単位で指定することにしよう。0は制限なしとする。
    # _url_pattern: str = ''              # 指定したパターンをurlに含むもので限定(正規表現)
    # _sitemap_continued: bool = False    # 前回の続きから(最後に取得したlastmodの日時)
    # _sitemap_term_days: int = 0         # 当日を含め、指定した日数を含むsitemapに限定
    # _url_term_days: int = 0             # 当日を含め、指定した日数を含むurlに限定
    # _debug_flg = False                  # debugモードを引数で指定された場合True
    # MongoDB関連
    mongo: MongoModel                   # MongoDBへの接続を行うインスタンスをspider内に保持。pipelinesで使用。
    _crawler_controller_recode: Any     # crawler_controllerコレクションから取得した対象ドメインのレコード
    # スパイダーの挙動制御関連、固有の情報など
    # _until_this_time: datetime          # sitemapで対象とするlastmodに制限をかける時間
    _crawl_start_time: datetime         # Scrapy起動時点の基準となる時間
    _crawl_start_time_iso: str          # Scrapy起動時点の基準となる時間(ISOフォーマットの文字列)
    # sitemapで最初の1件目にあるlastmod(最新の更新)を保持。pipelineで使用。
    _domain_name = 'sample_com'         # 各種処理で使用するドメイン名の一元管理。継承先で上書き要。
    #_latest_lastmod: str
    # _latest_url: str                    # 上記'latest_lastmod'のurl
    # _spider_type: str = 'SitemapSpider'  # spiderの種類。pipelineで使用。

    # sitemap_urlsに複数のサイトマップを指定した場合、その数だけsitemap_filterが可動する。その際、どのサイトマップか判別できるように処理中のサイトマップと連動するカウント。
    _sitemap_urls_count: int = 0
    # crawler_controllerコレクションへ書き込むレコードのdomain以降のレイアウト雛形。※最上位のKeyのdomainはサイトの特性にかかわらず固定とするため。
    _sitemap_next_crawl_info: dict = {name: {}, }

    kwargs_save: dict                    # 取得した引数を保存

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.mongo = MongoModel()
        self.kwargs_save: dict = kwargs

        # 引数チェック・保存
        self._crawler_controller_recode = self.__argument_check(
            self.mongo, self._domain_name, *args, **kwargs)

        # クロール開始時間
        self._crawl_start_time = datetime.now().astimezone(
            TIMEZONE)
        self._crawl_start_time_iso = self._crawl_start_time.isoformat()

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
        crawler_controller = CrawlerControllerModel(self.mongo)
        crawler_controller.update(
            {'domain': self._domain_name},
            {'domain': self._domain_name,
             self.name: self._sitemap_next_crawl_info[self.name],
             },
        )

        self.mongo.close()

    # def __argument_check(self, mongo: MongoModel, domain_name: str, *args, **kwargs) -> Tuple[bool, int, int, str, int, Any, bool]:

    def __argument_check(self, mongo: MongoModel, domain_name: str, *args, **kwargs) -> Any:
        '''あとで
        '''
        # debug_flg: bool = False                 # debugモードを引数で指定された場合True
        # url_term_days: int = 0                  # 当日を含め、指定した日数を含むurlに限定
        # sitemap_term_days: int = 0              # 当日を含め、指定した日数を含むsitemapに限定
        # url_pattern: str = ''                   # 指定したパターンをurlに含むもので限定(正規表現)
        # lastmod_recent_time: int = 0            # 直近の15分など。分単位で指定することにしよう。0は制限なしとする。
        crawler_controller_recode: Any = None   # crawler_controllerコレクションのレコード
        # sitemap_continued: bool = False         # 前回の続きから(最後に取得したlastmodの日時)

        ### 単項目チェック ###
        if 'debug' in kwargs:
            if kwargs['debug'] == 'Yes':
                #debug_flg = True
                self.logger.info('=== debugモード ON: %s', self.name)
                # デバック用のファイルを初期化
                _ = open('debug_entries.txt', 'w')
                _.close()
            else:
                sys.exit('引数エラー：debugに指定できるのは"Yes"のみです。')
        if 'url_term_days' in kwargs:
            if not kwargs['url_term_days'].isdecimal():
                #url_term_days = int(kwargs['url_term_days'])
                sys.exit('引数エラー：url_term_daysは数字のみ使用可。日単位で指定してください。')
            elif kwargs['url_term_days'] == 0:
                sys.exit('引数エラー：url_term_daysは0日の指定は不可です。1日以上を指定してください。')
        if 'sitemap_term_days' in kwargs:
            if not kwargs['sitemap_term_days'].isdecimal():
                sys.exit('引数エラー：sitemap_term_daysは数字のみ使用可。日単位で指定してください。')
            elif kwargs['sitemap_term_days'] == 0:
                sys.exit('引数エラー：sitemap_term_daysは0日の指定は不可です。1日以上を指定してください。')
        # if 'url_pattern' in kwargs:
        #    url_pattern = kwargs['url_pattern']
        if 'lastmod_recent_time' in kwargs:
            if not kwargs['lastmod_recent_time'].isdecimal():
                sys.exit('引数エラー：lastmod_recent_timeは数字のみ使用可。分単位で指定してください。')
            elif kwargs['lastmod_recent_time'] == 0:
                sys.exit('引数エラー：lastmod_recent_timeは0分の指定は不可です。')
            else:
                # lastmod_recent_time = int(kwargs['lastmod_recent_time'])
                pass
        if 'sitemap_continued' in kwargs:
            if kwargs['sitemap_continued'] == 'Yes':
                _crawler_controller = CrawlerControllerModel(mongo)
                crawler_controller_recode = _crawler_controller.find_one(
                    {'domain': domain_name})
                if crawler_controller_recode == None:
                    sys.exit('引数エラー：domain = ' + domain_name +
                             ' は前回のcrawl情報がありません。初回から"sitemap_continued"の使用は不可です。')
                # else:
                #    sitemap_continued = True
            else:
                sys.exit('引数エラー：sitemap_continuedに使用できるのは、"Yes"のみです。')

        ### 項目関連チェック ###
        if 'lastmod_recent_time' in kwargs and 'sitemap_continued' in kwargs:
            sys.exit('引数エラー：lastmod_recent_timeとsitemap_continuedは同時には使えません。')

        # return debug_flg, url_term_days, sitemap_term_days, url_pattern, lastmod_recent_time, crawler_controller_recode, sitemap_continued
        return crawler_controller_recode

    def __argument_processing(self):
        pass

    def sitemap_filter_common_prosses(self, entries):
        ''' 独自に拡張。
        １．起動時の引数に応じた処理を行うため、以下の抽出に使う変数を求める。
          => self._crawl_start_time, self._crawl_start_time_iso, self._until_this_time, self._tarm_days_list*, self._until_this_time
        ２．デバックモードが指定された場合、entriesをdebug_entries.txtへ出力する。
        '''

        # sitemap調査用。debugモードの場合のみ。
        if 'debug' in self.kwargs_save:
            _ = open('debug_entries.txt', 'a')
            for _entry in entries:
                _.write(_entry['lastmod'] + ', ' + _entry['loc'] + '\n')
            _.close()

        # 前回からの続きの指定がある場合
        # if self._sitemap_continued:
        #    self._until_this_time = parser.parse(
        #        self._crawler_controller_recode[self.name][self.sitemap_urls[self._sitemap_urls_count]]['latest_lastmod'])

    def term_days_Calculation(self, crawl_start_time: datetime, term_days: int, date_pattern: str) -> list:
        '''
        クロール開始時刻(crawl_start_time)を起点に、期間(term_days)分の日付リスト(文字列)を返す。
        日付の形式(date_pattern)には、datetime.strftimeのパターンを指定する。
        '''
        return [(crawl_start_time - timedelta(days=i)).strftime(date_pattern) for i in range(term_days)]

        # if 'url_term_days' in self.kwargs_save:
        #     self._url_tarm_days_list_yymmdd: list = [
        #         (self._crawl_start_time - timedelta(days=i)).strftime('%y%m%d') for i in range(int(self.kwargs_save['url_term_days']))]
        #     self._url_tarm_days_list_yyyymmdd: list = [
        #         (self._crawl_start_time - timedelta(days=i)).strftime('%Y%m%d') for i in range(self._url_term_days)]
        #     self._url_tarm_days_list_yy_mm_dd: list = [
        #         (self._crawl_start_time - timedelta(days=i)).strftime('%y-%m-%d') for i in range(self._url_term_days)]
        #     self._url_tarm_days_list_yyyy_mm_dd: list = [
        #         (self._crawl_start_time - timedelta(days=i)).strftime('%Y-%m-%d') for i in range(self._url_term_days)]
