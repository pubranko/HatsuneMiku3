# 単一プロセスでcrawlerprocessを使った例
import os
import sys
import io
import re
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from prefect import Flow, task
import logging
from datetime import datetime

prefect_flow_name: str = 'RegularObservation'
log_file_path = os.path.join(
    'logs', 'regular_observation_spider.log')
logging.basicConfig(level=logging.DEBUG, filemode="w+", filename=log_file_path,
                    format='%(asctime)s [%(name)s] %(levelname)s: %(message)s', datefmt='%Y-%m-%d %H:%M:%S')


@task
def regular_observation():
    '''定期観測'''

    path = os.getcwd()
    sys.path.append(path)
    from news_crawl.spiders.epochtimes_jp_sitemap import EpochtimesJpSitemapSpider
    from news_crawl.spiders.sankei_com_sitemap import SankeiComSitemapSpider
    from news_crawl.models.mongo_model import MongoModel
    from news_crawl.models.crawler_logs_model import CrawlerLogsModel
    from news_crawl.settings import TIMEZONE
    from common.mail_send import mail_send

    # MongoDBオープン
    mongo = MongoModel()

    # クロール開始時間
    crawl_start_time = datetime.now().astimezone(
        TIMEZONE)

    process = CrawlerProcess(settings=get_project_settings())
    process.crawl(SankeiComSitemapSpider,
                  crawl_start_time=crawl_start_time,
                  debug='Yes',
                  lastmod_recent_time='10',
                  url_term_days='1',)
    process.crawl(EpochtimesJpSitemapSpider,
                  crawl_start_time=crawl_start_time,
                  debug='Yes',
                  lastmod_recent_time='10',)
    process.start()

    # logファイルオープン
    with open(log_file_path) as f:
        log_file = f.read()
    # クロール結果のログをMongoDBへ保存
    crawler_logs = CrawlerLogsModel(mongo)
    crawler_logs.insert_one({
        'crawl_start_time': crawl_start_time.isoformat(),
        'record_type': 'regular_observation_spider',
        'logs': log_file,
    })

    #pattern = re.compile(r'^\[[0-9]+,[0-9]+\]$')
    #CRITICAL > ERROR > WARNING > INFO > DEBUG
    pattern_critical = re.compile(
        r'^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2} \[.*\] CRITICAL')
    pattern_error = re.compile(
        r'^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2} \[.*\] ERROR')
    pattern_warning = re.compile(
        r'^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2} \[.*\] WARNING')
    # 2021-07-31 20:43:48 [twisted] CRITICAL: Unhandled error in Deferred:

    logs = io.StringIO(log_file)
    title: str = ''
    msg: str = '\n'.join([
        '【ログ】', log_file,
    ])

    for line in logs.readlines():
        if pattern_critical.search(line):
            title = '【クリティカル発生】' + crawl_start_time.isoformat()
        elif pattern_error.search(line):
            title = '【エラー発生】' + crawl_start_time.isoformat()
        elif pattern_warning.search(line):
            title = '【ワーニング発生】' + crawl_start_time.isoformat()

    if not title == '':
        mail_send(title, msg,)

    # 終了処理
    mongo.close()


with Flow(prefect_flow_name) as flow:
    regular_observation()

flow.run()