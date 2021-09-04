from datetime import datetime
from dateutil import parser
from logging import Logger
from scrapy.exceptions import CloseSpider
from models.mongo_model import MongoModel
from models.controller_model import ControllerModel
from news_crawl.settings import TIMEZONE
from news_crawl.spiders.common.environ_check import environ_check
from news_crawl.spiders.common.argument_check import argument_check
from news_crawl.spiders.common.start_request_debug_file_init import start_request_debug_file_init
from news_crawl.spiders.common.crawling_domain_duplicate_check import CrawlingDomainDuplicatePrevention
from news_crawl.spiders.common.lastmod_period_skip_check import LastmodPeriodMinutesSkipCheck
from news_crawl.spiders.common.crawling_continued_skip_check import CrawlingContinuedSkipCheck
from common_lib.resource_check import resource_check


def spider_init(spider, *args, **kwargs):
    '''spider共通の初期処理'''
    domain_name: str = spider._domain_name
    name: str = spider.name
    logger: Logger = spider.logger
    crawling_start_time: datetime

    # 必要な環境変数チェック
    environ_check()
    # MongoDBオープン
    spider.mongo = MongoModel()
    # 前回のドメイン別のクロール結果を取得
    _controller = ControllerModel(spider.mongo)
    spider._crawl_point = _controller.crawl_point_get(
        domain_name, name)

    # 引数保存・チェック
    spider.kwargs_save = kwargs
    argument_check(
        spider, domain_name, spider._crawl_point, *args, **kwargs)

    # 同一ドメインへの多重クローリングを防止
    spider.crawling_domain_control = CrawlingDomainDuplicatePrevention()
    duplicate_check = spider.crawling_domain_control.execution(
        domain_name)
    if not duplicate_check:
        raise CloseSpider('同一ドメインへの多重クローリングとなるため中止')

    resource: dict = resource_check()
    # CPUチェック
    if float(str(resource['cpu_percent'])) > 90:
        logger.warning('=== ＣＰＵ使用率が90%を超えています。')
    # メモリチェック
    if float(str(resource['memory_percent'])) > 85:
        logger.warning('=== メモリー使用率が85%を超えています。')
    elif float(str(resource['memory_percent'])) > 95:
        raise CloseSpider('=== メモリー使用率が95%を超えたためスパイダーを停止します。')
    # スワップメモリチェック
    if float(str(resource['swap_memory_percent'])) > 85:
        logger.warning('=== スワップメモリー使用率が85%を超えています。')
    elif float(str(resource['swap_memory_percent'])) > 95:
        raise CloseSpider('=== スワップメモリー使用率が95%を超えたためスパイダーを停止します。')

    # クロール開始時間をスパイダーのクラス変数に保存
    if 'crawling_start_time' in spider.kwargs_save:
        crawling_start_time = parser.parse(
            spider.kwargs_save['crawling_start_time'])
    else:
        crawling_start_time = datetime.now().astimezone(TIMEZONE)

    spider._crawling_start_time = crawling_start_time

    logger.info(
        '=== __init__ : 開始時間(%s)' % (crawling_start_time.isoformat()))
    logger.info(
        '=== __init__ : 引数(%s)' % (kwargs))
    logger.info(
        '=== __init__ : 今回向けクロールポイント情報 \n %s', spider._crawl_point)

    start_request_debug_file_init(spider, spider.kwargs_save)

    # チェック用クラスの初期化＆スパイダーのクラス変数に保存
    spider.lastmod_pefiod = LastmodPeriodMinutesSkipCheck(
            spider, spider._crawling_start_time, kwargs)
    spider.crawling_continued = CrawlingContinuedSkipCheck(
        spider._crawl_point, kwargs)
