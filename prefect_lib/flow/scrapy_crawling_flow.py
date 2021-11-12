import os
import sys
from datetime import datetime
import logging
from prefect import Flow, task, Parameter
from prefect.tasks.control_flow.conditional import ifelse
from prefect.engine import signals
from prefect.utilities.context import Context
path = os.getcwd()
sys.path.append(path)
from prefect_lib.task.scrapy_crawling_task import ScrapyCrawlingTask
from prefect_lib.settings import TIMEZONE
from prefect_lib.common_module.flow_status_change import flow_status_change

start_time = datetime.now().astimezone(
    TIMEZONE)
log_file_path = os.path.join(
    'logs', os.path.splitext(os.path.basename(__file__))[0] + '.log')
logging.basicConfig(level=logging.DEBUG, filemode="w+", filename=log_file_path,
                    format='%(asctime)s %(levelname)s [%(name)s] : %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

with Flow(
    name='Scrapy crawling flow',
    state_handlers=[flow_status_change],
) as flow:
    spider_names = Parameter('spider_names')()
    spider_kwargs = Parameter('spider_kwargs')()
    following_processing_execution = Parameter(
        'following_processing_execution', default='No')()
    task = ScrapyCrawlingTask(
        log_file_path=log_file_path, start_time=start_time)
    result = task(spider_names=spider_names, spider_kwargs=spider_kwargs,
                  following_processing_execution=following_processing_execution)

flow.run(parameters=dict(
    spider_names=[
        'sankei_com_sitemap', 'asahi_com_sitemap', 'kyodo_co_jp_sitemap', 'jp_reuters_com_crawl', 'yomiuri_co_jp_sitemap', 'epochtimes_jp_sitemap',
        #'jp_reuters_com_crawl',
        #'sankei_com_sitemap', 'asahi_com_sitemap', 'kyodo_co_jp_sitemap', 'yomiuri_co_jp_sitemap', 'epochtimes_jp_sitemap',
    ],
    spider_kwargs={
        'debug': 'Yes',
        'lastmod_period_minutes': '20,',
        'pages': '2,2',
        # 'continued':'Yes',
        # 'direct_crawl_urls':[],
        # 'crawl_point_non_update':'Yes',
    },
    following_processing_execution='Yes'    # 後続処理実行(scrapying,news_clip_masterへの登録,solrへの登録)
))
