from typing import Any
import os
import pickle
from scrapy.spiders import XMLFeedSpider
from scrapy.http import Response
from datetime import datetime, timedelta
from scrapy.http import Response
from scrapy.http.response.xml import XmlResponse
from news_crawl.models.mongo_model import MongoModel
from news_crawl.models.crawler_controller_model import CrawlerControllerModel
from news_crawl.spiders.function.argument_check import argument_check
from news_crawl.settings import TIMEZONE
from scrapy.utils.spider import iterate_spider_output
from news_crawl.items import NewsCrawlItem


class ExtensionsXmlFeedSpider(XMLFeedSpider):
    name: str = 'sample_com_xml_feed'                           # 継承先で上書き要。
    allowed_domains: list = ['sample.com']                      # 継承先で上書き要。
    start_urls: list = ['https://www.sample.com/sitemap.xml', ]  # 継承先で上書き要。
    custom_settings: dict = {
        'DEPTH_LIMIT': 2,
        'DEPTH_STATS_VERBOSE': True
    }
    _spider_version: float = 0.0          # spiderのバージョン。継承先で上書き要。
    _extensions_xml_version: float = 1.0         # 当クラスのバージョン

    # 引数の値保存
    # MongoDB関連
    mongo: MongoModel                   # MongoDBへの接続を行うインスタンスをspider内に保持。pipelinesで使用。
    _crawler_controller_recode: Any     # crawler_controllerコレクションから取得した対象ドメインのレコード
    # スパイダーの挙動制御関連、固有の情報など
    _crawl_start_time: datetime         # Scrapy起動時点の基準となる時間
    _crawl_start_time_iso: str          # Scrapy起動時点の基準となる時間(ISOフォーマットの文字列)
    _domain_name = 'sample_com'         # 各種処理で使用するドメイン名の一元管理。継承先で上書き要。

    _entities: list = []
    _request_list: list = []
    _xml_next_crawl_info: dict = {name: {}, }
    # Trueの場合、継承先でオーバーライドしたcustom_url()メソッドを使い、urlをカスタムする。
    _custom_url_flg: bool = False

    # 処理中のサイトマップ内で、最大のlastmodとurlを記録するエリア
    _max_lstmod: str = ''
    _max_url: str = ''
    _xml_extract_count :int = 0

    iterator: str = 'iternodes'
    itertag: str = 'url'

    def __init__(self, *args, **kwargs):
        ''' (拡張メソッド)
        親クラスの__init__処理後に追加で初期処理を行う。
        '''
        super().__init__(*args, **kwargs)
        # MongoDBオープン
        self.mongo = MongoModel()
        # 前回のドメイン別のクロール結果を取得
        _crawler_controller = CrawlerControllerModel(self.mongo)
        self._crawler_controller_recode = _crawler_controller.find_one(
            {'domain': self._domain_name})
        self.logger.info(
            '=== __init__ : crawler_controllerにある前回情報 \n %s', self._crawler_controller_recode)
        # 前回のクロール情報を次回向けの初期値とする。
        self._xml_next_crawl_info: dict = {self.name: {}, }
        if not self._crawler_controller_recode == None :
            if self.name in self._crawler_controller_recode:
                self._xml_next_crawl_info[self.name] = self._crawler_controller_recode[self.name]

        # 引数保存・チェック
        self.kwargs_save: dict = kwargs
        argument_check(
            self, self._domain_name, self._crawler_controller_recode, *args, **kwargs)

        # クロール開始時間
        self._crawl_start_time = datetime.now().astimezone(
            TIMEZONE)
        self._crawl_start_time_iso = self._crawl_start_time.isoformat()

    def parse_news(self, response: Response):
        ''' (拡張メソッド)
        取得したレスポンスよりDBへ書き込み
        '''
        _info = self.name + ':' + str(self._spider_version) + ' / ' \
            + 'extensions_crawl:' + str(self._extensions_xml_version)

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
        self.logger.info('=== Spider closed: %s', self.name)

        self.xml_node_debug_file_generate(self._entities)

        crawler_controller = CrawlerControllerModel(self.mongo)
        crawler_controller.update(
            {'domain': self._domain_name},
            {'domain': self._domain_name,
             self.name: self._xml_next_crawl_info[self.name],
             },
        )

        _ = crawler_controller.find_one(
            {'domain': self._domain_name})
        self.logger.info(
            '=== closed : crawler_controllerに次回情報を保存 \n %s', _)

        self.mongo.close()

    def xml_node_debug_file_generate(self, entries: list):
        ''' (拡張メソッド)
        あとで
        '''
        if 'debug' in self.kwargs_save:         # sitemap調査用。debugモードの場合のみ。
            path: str = os.path.join(
                'debug', 'start_urls(' + self.name + ').txt')
            with open(path, 'a') as _:
                for _entry in entries:
                    _entry: list
                    _.write(_entry[0] + ',' + _entry[1] +
                            ',' + _entry[2] + '\n')

    def term_days_Calculation(self, crawl_start_time: datetime, term_days: int, date_pattern: str) -> list:
        ''' (拡張メソッド)
        クロール開始時刻(crawl_start_time)を起点に、期間(term_days)分の日付リスト(文字列)を返す。
        日付の形式(date_pattern)には、datetime.strftimeのパターンを指定する。
        '''
        return [(crawl_start_time - timedelta(days=i)).strftime(date_pattern) for i in range(term_days)]

    def parse_nodes(self, response: XmlResponse, nodes):
        """(オーバーライド)
        各xmlファイルの初期処理、主処理、終了処理を記述可能
        """
        # 初期処理
        self._xml_extract_count = 0 #xmlごとに何件対象となったか確認するためのカウンター初期化
        # 主処理
        for selector in nodes:
            ret: Any = iterate_spider_output(
                self.parse_node(response, selector))
            for result_item in self.process_results(response, ret):
                yield result_item
        # 終了処理
        self.logger.info(
            '=== parse_nodes : XMLの解析完了 : 件数 = %s ,url = %s ', self._xml_extract_count,response.url)
        # サイトマップごとの最大更新時間を記録(crawler_controllerコレクションへ保存する内容)
        self._xml_next_crawl_info[self.name][response.url] = {
            'latest_lastmod': self._max_lstmod,
            'latest_url': self._max_url,
            'crawl_start_time': self._crawl_start_time_iso,
        }

    def _custom_url(self, url: str) -> str:
        ''' (拡張メソッド)
        sitemapのurlをカスタマイズしたい場合、継承先でオーバーライドして使用する。
        '''
        return url
