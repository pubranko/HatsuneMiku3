from __future__ import annotations  # ExtensionsSitemapSpiderの循環参照を回避するため
from typing import Union, TYPE_CHECKING, Any
from scrapy.exceptions import CloseSpider
from BrownieAtelierMongo.collection_models.mongo_model import MongoModel
from BrownieAtelierMongo.collection_models.controller_model import ControllerModel
from news_crawl.spiders.common.start_request_debug_file_init import start_request_debug_file_init
from news_crawl.spiders.common.crawling_domain_duplicate_check import CrawlingDomainDuplicatePrevention
from news_crawl.spiders.common.lastmod_term_skip_check import LastmodTermSkipCheck
from news_crawl.spiders.common.lastmod_continued_skip_check import LastmodContinuedSkipCheck
from news_crawl.news_crawl_input import NewsCrawlInput
from shared.resource_check import resource_check

if TYPE_CHECKING:  # 型チェック時のみインポート
    from news_crawl.spiders.extensions_class.extensions_sitemap import ExtensionsSitemapSpider
    from news_crawl.spiders.extensions_class.extensions_crawl import ExtensionsCrawlSpider
    #from news_crawl.spiders.extensions_class.extensions_xml_feed import ExtensionsXmlFeedSpider

from logging import Logger,LoggerAdapter
def spider_init(
    spider: Union[ExtensionsSitemapSpider, ExtensionsCrawlSpider],
    *args, **kwargs
):
    '''spider共通の初期処理'''
    domain_name: str = spider._domain_name
    spider_name: str = spider.name
    # logger = logging.getLogger(spider.name)
    #logger: LoggerAdapter = spider.logger

    spider.logger.info(
        '=== spider_init : ' + spider_name + '開始')

    # MongoDBオープン
    spider.logger.logger
    spider.logger.logger
    spider.mongo = MongoModel(spider.logger.logger)     # MongoModelではLoggerAdapterではなくLoggerで定義している。そのためとりあえずLoggerを渡すよう対応中
    # コントローラーモデルを生成
    controller = ControllerModel(spider.mongo)

    # 引数の保存＆チェックを行う
    spider.news_crawl_input = NewsCrawlInput(**kwargs)

    # クロール対象となるurlと付随する情報（サイトマップurl、lastmodなど）を保存。
    # spiderを並列で実行する際、extensions_~.pyの定義のみでは全て共有されてしまいデータが混在する。
    # 初期処理で各spiderのインスタンスへ直接値を入れることで混在を避けている。
    spider.crawl_urls_list = []

    # 同一ドメインへの多重クローリングを防止
    crawling_domain_control = CrawlingDomainDuplicatePrevention()
    duplicate_check = crawling_domain_control.execution(
        domain_name)
    if not duplicate_check:
        raise CloseSpider('同一ドメインへの多重クローリングとなるため中止')

    resource: dict = resource_check()
    # CPUチェック
    if float(str(resource['cpu_percent'])) > 90:
        spider.logger.warning('=== CPU使用率が90%を超えています。')
    # メモリチェック
    if float(str(resource['memory_percent'])) > 85:
        spider.logger.warning('=== メモリー使用率が85%を超えています。')
    elif float(str(resource['memory_percent'])) > 95:
        raise CloseSpider('=== メモリー使用率が95%を超えたためスパイダーを停止します。')
    # スワップメモリチェック
    if float(str(resource['swap_memory_percent'])) > 85:
        spider.logger.warning('=== スワップメモリー使用率が85%を超えています。')
    elif float(str(resource['swap_memory_percent'])) > 95:
        raise CloseSpider('=== スワップメモリー使用率が95%を超えたためスパイダーを停止します。')

    spider.logger.info(
        f'=== __init__ : 開始時間({spider.news_crawl_input.crawling_start_time.isoformat()})')
    spider.logger.info(
        f'=== __init__ : 引数({kwargs})')
    spider.logger.info(
        f'=== __init__ : 今回向けクロールポイント情報 \n {spider._crawl_point}')

    start_request_debug_file_init(spider, spider.news_crawl_input.debug)

    # チェック用クラスの初期化＆スパイダーのクラス変数に保存
    spider.lastmod_term = LastmodTermSkipCheck(
        spider,
        spider.news_crawl_input.crawling_start_time,
        spider.news_crawl_input.lastmod_term_minutes_from,
        spider.news_crawl_input.lastmod_term_minutes_to)

    # 前回の続きからクロールする
    spider.lastmod_continued = LastmodContinuedSkipCheck(
        spider.news_crawl_input.continued,
        spider_name,
        domain_name,
        controller,
        spider.logger)
