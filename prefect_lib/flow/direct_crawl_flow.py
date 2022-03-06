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
from prefect_lib.common_module.logging_setting import log_file_path
from prefect_lib.common_module.flow_status_change import flow_status_change
from prefect_lib.task.direct_crawl_task import DirectCrawlTask

'''
ダイレクトクロール
・指定したurlへのみクロールを行う。
・事前にstatic/direct_crawl_files配下のファイルにクロールしたいurlを登録する。
・実行時にそのファイル名を指定する。
'''
with Flow(
    name='Direct crawl flow',
    state_handlers=[flow_status_change],
) as flow:

    spider_name = Parameter('spider_name', required=True)()
    file = Parameter('file', required=False)()
    task = DirectCrawlTask(
        log_file_path=log_file_path, start_time=datetime.now().astimezone(TIMEZONE))
    result = task(spider_name=spider_name, file=file,)

# scraped_save_start_time_*による絞り込みは任意
flow.run(parameters=dict(
    spider_name='sankei_com_sitemap',
    file='sankei_com(test).txt',
    # domain='',
    #scrapying_start_time_from=datetime(2021, 8, 21, 0, 0, 0).astimezone(TIMEZONE),
    #scrapying_start_time_to=datetime(2021, 8, 21, 10, 18, 12, 160000).astimezone(TIMEZONE),
    #scrapying_start_time_from=datetime(2021, 8, 21, 10, 18, 12, 161000).astimezone(TIMEZONE),
    #scrapying_start_time_to=datetime(2021, 8, 21, 10, 18, 12, 160000).astimezone(TIMEZONE),
))
