from bs4.element import ResultSet
from scrapy.http import Response
from typing import Any
from scrapy.spiders import CrawlSpider
from datetime import datetime
import pickle
import sys
import os
from news_crawl.items import NewsCrawlItem
from news_crawl.models.mongo_model import MongoModel
from news_crawl.models.crawler_controller_model import CrawlerControllerModel
from datetime import datetime, timedelta
from news_crawl.settings import TIMEZONE
from bs4 import BeautifulSoup as bs4


class ExtensionsCrawlSpider(CrawlSpider):
    '''
    CrawlSpiderの機能を拡張したクラス。
    Override → __init__(),parse(),closed()
    (前提)
    name, allowed_domains, start_urls, _domain_name, spider_versionの値は当クラスを継承するクラスで設定すること。
    '''

    name: str = 'extension_crawl'                                 # 継承先で上書き要。
    allowed_domains: list = ['sample.com']                          # 継承先で上書き要。
    start_urls: list = ['https://www.sample.com/crawl.html', ]   # 継承先で上書き要。
    custom_settings: dict = {
        'DEPTH_LIMIT': 2,
        'DEPTH_STATS_VERBOSE': True
    }
    spider_version: float = 0.0          # spiderのバージョン。継承先で上書き要。
    _extensions_crawl_version: float = 1.0         # 当クラスのバージョン

    # 引数の値保存
    # MongoDB関連
    mongo: MongoModel                   # MongoDBへの接続を行うインスタンスをspider内に保持。pipelinesで使用。
    _crawler_controller_recode: Any     # crawler_controllerコレクションから取得した対象ドメインのレコード
    # スパイダーの挙動制御関連、固有の情報など
    _crawl_start_time: datetime         # Scrapy起動時点の基準となる時間
    _crawl_start_time_iso: str          # Scrapy起動時点の基準となる時間(ISOフォーマットの文字列)
    _domain_name = 'sample_com'         # 各種処理で使用するドメイン名の一元管理。継承先で上書き要。

    # start_urlsに複数のurlを指定した場合、どのurlの処理中か判別できるようにカウントする。
    _crawl_start_urls_count: int = 0
    # crawler_controllerコレクションへ書き込むレコードのdomain以降のレイアウト雛形。※最上位のKeyのdomainはサイトの特性にかかわらず固定とするため。
    _crawl_next_info: dict = {name: {}, }

    kwargs_save: dict                    # 取得した引数を保存

    def __init__(self, *args, **kwargs):
        ''' (拡張メソッド)
        親クラスの__init__処理後に追加で初期処理を行う。
        '''
        super().__init__(*args, **kwargs)
        self.mongo = MongoModel()
        self.kwargs_save: dict = kwargs

        # 前回のドメイン別のクロール結果を取得
        _crawler_controller = CrawlerControllerModel(self.mongo)
        self._crawler_controller_recode = _crawler_controller.find_one(
            {'domain': self._domain_name})
        self.logger.info(
            '=== __init__ : crawler_controllerにある前回情報 \n %s', self._crawler_controller_recode)

        # 引数チェック・保存
        self.__argument_check(
            self._domain_name, self._crawler_controller_recode, *args, **kwargs)

        # クロール開始時間
        self._crawl_start_time = datetime.now().astimezone(
            TIMEZONE)
        self._crawl_start_time_iso = self._crawl_start_time.isoformat()

    def __argument_check(self, domain_name: str, crawler_controller_recode: Any, *args, **kwargs) -> None:
        ''' (拡張メソッド)
        引数のチェックを行う。
        '''
        ### 単項目チェック ###
        if 'debug' in kwargs:
            if kwargs['debug'] == 'Yes':
                self.logger.info('=== debugモード ON: %s', self.name)
                # デバック用のファイルを初期化
                path = os.path.join(
                    'news_crawl', 'debug', 'debug_start_url(' + self.name + ').txt')
                with open(path, 'w') as _:
                    pass
            else:
                sys.exit('引数エラー：debugに指定できるのは"Yes"のみです。')
        if 'url_term_days' in kwargs:
            if not kwargs['url_term_days'].isdecimal():
                sys.exit('引数エラー：url_term_daysは数字のみ使用可。日単位で指定してください。')
            elif kwargs['url_term_days'] == 0:
                sys.exit('引数エラー：url_term_daysは0日の指定は不可です。1日以上を指定してください。')
        if 'lastmod_recent_time' in kwargs:
            if not kwargs['lastmod_recent_time'].isdecimal():
                sys.exit('引数エラー：lastmod_recent_timeは数字のみ使用可。分単位で指定してください。')
            elif kwargs['lastmod_recent_time'] == 0:
                sys.exit('引数エラー：lastmod_recent_timeは0分の指定は不可です。')

        ### 項目関連チェック ###
        # pass

    # def parse_start_url(self, response: Response):
    #     '''
    #     start_urls自体のレスポンスの処理
    #     '''
    #     self.common_prosses(self.start_urls[self._crawl_urls_count], response)
    #     self._crawl_urls_count += 1  # 次のurl用にカウントアップ

    def common_prosses(self, start_url, urls_list: list):
        ''' (拡張メソッド)
        デバックモードが指定された場合、entriesと元となったsitemapのurlをdebug_entries.txtへ出力する。
        '''
        if 'debug' in self.kwargs_save:         # sitemap調査用。debugモードの場合のみ。
            path: str = os.path.join(
                'news_crawl', 'debug', 'debug_start_url(' + self.name + ').txt')
            with open(path, 'a') as _:
                for url in urls_list:
                    _.write(start_url + ',' + url + '\n')

    def term_days_Calculation(self, crawl_start_time: datetime, term_days: int, date_pattern: str) -> list:
        ''' (拡張メソッド)
        クロール開始時刻(crawl_start_time)を起点に、期間(term_days)分の日付リスト(文字列)を返す。
        日付の形式(date_pattern)には、datetime.strftimeのパターンを指定する。
        '''
        return [(crawl_start_time - timedelta(days=i)).strftime(date_pattern) for i in range(term_days)]

    def parse_news(self, response: Response):
        ''' (拡張メソッド)
        取得したレスポンスよりDBへ書き込み
        '''
        _info = self.name + ':' + str(self.spider_version) + ' / ' \
            + 'extensions_crawl:' + str(self._extensions_crawl_version)

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
        crawler_controller.update(
            {'domain': self._domain_name},
            {'domain': self._domain_name,
             self.name: self._crawl_next_info[self.name],
             },
        )

        _ = crawler_controller.find_one(
            {'domain': self._domain_name})
        self.logger.info(
            '=== closed : crawler_controllerに次回情報を保存 \n %s', _)

        self.logger.info('=== Spider closed: %s', self.name)
        self.mongo.close()
