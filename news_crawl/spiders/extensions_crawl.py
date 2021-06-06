import pickle
from typing import Any
from datetime import datetime
from scrapy.spiders import CrawlSpider
from scrapy.http import Response
from news_crawl.items import NewsCrawlItem
from news_crawl.models.mongo_model import MongoModel
from news_crawl.models.crawler_controller_model import CrawlerControllerModel
from news_crawl.settings import TIMEZONE
from news_crawl.spiders.function.argument_check import argument_check
from news_crawl.spiders.function.start_request_debug_file_init import start_request_debug_file_init


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
        'DEPTH_STATS_VERBOSE': True
    }
    _spider_version: float = 0.0          # spiderのバージョン。継承先で上書き要。
    _extensions_crawl_version: float = 1.0         # 当クラスのバージョン

    # 引数の値保存
    kwargs_save: dict                    # 取得した引数を保存
    # MongoDB関連
    mongo: MongoModel                   # MongoDBへの接続を行うインスタンスをspider内に保持。pipelinesで使用。
    _crawler_controller_recode: Any     # crawler_controllerコレクションから取得した対象ドメインのレコード
    # スパイダーの挙動制御関連、固有の情報など
    _crawl_start_time: datetime         # Scrapy起動時点の基準となる時間
    _domain_name = 'sample_com'         # 各種処理で使用するドメイン名の一元管理。継承先で上書き要。

    # crawler_controllerコレクションへ書き込むレコードのdomain以降のレイアウト雛形。※最上位のKeyのdomainはサイトの特性にかかわらず固定とするため。
    _next_crawl_info: dict = {name: {}, }

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
        self._next_crawl_info: dict = {self.name: {}, }
        if not self._crawler_controller_recode == None:
            if self.name in self._crawler_controller_recode:
                self._next_crawl_info[self.name] = self._crawler_controller_recode[self.name]

        # 引数保存・チェック
        self.kwargs_save: dict = kwargs
        argument_check(
            self, self._domain_name, self._crawler_controller_recode, *args, **kwargs)

        start_request_debug_file_init(self, self.kwargs_save)

        # クロール開始時間
        self._crawl_start_time = datetime.now().astimezone(
            TIMEZONE)

    def parse_news(self, response: Response):
        ''' (拡張メソッド)
        取得したレスポンスよりDBへ書き込み
        '''
        _info = self.name + ':' + str(self._spider_version) + ' / ' \
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
        self._crawler_controller_recode = crawler_controller.find_one(
            {'domain': self._domain_name})
        if self._crawler_controller_recode == None:  # ドメインに対して初クロールの場合
            self._crawler_controller_recode = {
                'domain': self._domain_name,
                self.name: self._next_crawl_info[self.name]
            }
        else:
            self._crawler_controller_recode[self.name] = self._next_crawl_info[self.name]

        crawler_controller.update(
            {'domain': self._domain_name},
            self._crawler_controller_recode,
        )

        self.logger.info(
            '=== closed : crawler_controllerに次回情報を保存 \n %s', self._crawler_controller_recode)

        self.mongo.close()
        self.logger.info('=== Spider closed: %s', self.name)

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
            pages: list = eval(self.kwargs_save['pages'])
            return{'start_page': pages[0], 'end_page': pages[1]}
        else:
            return{'start_page': start_page, 'end_page': end_page}
