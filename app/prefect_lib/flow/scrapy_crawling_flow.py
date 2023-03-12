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
from prefect_lib.common_module.flow_status_change import flow_status_change
from prefect_lib.task.scrapy_crawling_task import ScrapyCrawlingTask

'''
scrapyによるクロールを実行する。
・対象のスパイダーを指定できる。
・スパイダーの実行時オプションを指定できる。
・後続のスクレイピング〜news_clipへの登録まで一括で実行するか選択できる。
'''
with Flow(
    name='[CRAWL_003] Scrapy crawling flow',
    state_handlers=[flow_status_change],
) as flow:
    spider_names = Parameter('spider_names')()
    spider_kwargs = Parameter('spider_kwargs')()
    following_processing_execution = Parameter(
        'following_processing_execution', default=False)()
    task = ScrapyCrawlingTask()
    result = task(spider_names=spider_names, spider_kwargs=spider_kwargs,
                  following_processing_execution=following_processing_execution)
