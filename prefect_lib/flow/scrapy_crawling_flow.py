import os
import sys
from datetime import datetime
from prefect.core.flow import Flow
from prefect.core.parameter import Parameter
from prefect.tasks.control_flow.conditional import ifelse
from prefect.engine import signals
from prefect.utilities.context import Context
path = os.getcwd()
sys.path.append(path)
from prefect_lib.settings import TIMEZONE
from prefect_lib.common_module.logging_setting import LOG_FILE_PATH
from prefect_lib.common_module.flow_status_change import flow_status_change
from prefect_lib.task.scrapy_crawling_task import ScrapyCrawlingTask

'''
scrapyによるクロールを実行する。
・対象のスパイダーを指定できる。
・スパイダーの実行時オプションを指定できる。
・後続のスクレイピング〜news_clipへの登録まで一括で実行するか選択できる。
'''
with Flow(
    name='Scrapy crawling flow',
    state_handlers=[flow_status_change],
) as flow:
    spider_names = Parameter('spider_names')()
    spider_kwargs = Parameter('spider_kwargs')()
    following_processing_execution = Parameter(
        'following_processing_execution', default='No')()
    task = ScrapyCrawlingTask(
        log_file_path=LOG_FILE_PATH, start_time=datetime.now().astimezone(TIMEZONE))
    result = task(spider_names=spider_names, spider_kwargs=spider_kwargs,
                  following_processing_execution=following_processing_execution)

flow.run(parameters=dict(
    spider_names=[
        'sankei_com_sitemap', 'asahi_com_sitemap', 'kyodo_co_jp_sitemap', 'jp_reuters_com_crawl', 'yomiuri_co_jp_sitemap', 'epochtimes_jp_sitemap',
        #'epochtimes_jp_sitemap',
        #'yomiuri_co_jp_sitemap',
        #'sankei_com_sitemap',
        #'asahi_com_sitemap',
    ],
    spider_kwargs={
        'debug': 'Yes',
        'lastmod_period_minutes': '120,',
        'pages': '1,2',
        # 'continued':'Yes',
        # 'direct_crawl_urls':[],
        'crawl_point_non_update':'Yes',
    },
    following_processing_execution='Yes'    # 後続処理実行(scrapying,news_clip_masterへの登録,solrへの登録)
))
