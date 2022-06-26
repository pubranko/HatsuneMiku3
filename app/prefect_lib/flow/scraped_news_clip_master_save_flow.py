import os
import sys
from datetime import datetime
from prefect.core.flow import Flow
from prefect.core.parameter import Parameter
from prefect.core.parameter import DateTimeParameter
from prefect.tasks.control_flow.conditional import ifelse
from prefect.engine import signals
from prefect.utilities.context import Context
path = os.getcwd()
sys.path.append(path)
from prefect_lib.settings import TIMEZONE
from prefect_lib.common_module.logging_setting import LOG_FILE_PATH
from prefect_lib.common_module.flow_status_change import flow_status_change
from prefect_lib.task.scraped_news_clip_master_save_task import ScrapedNewsClipMasterSaveTask

'''
スクレイピング結果をニュースクリップマスター(news_clip_master)へ登録する。
・対象のドメイン、スクレイピング時間を指定できる。
'''
with Flow(
    name='[CRAWL_006] Scraped news clip master save flow',
    state_handlers=[flow_status_change],
) as flow:
    domain = Parameter('domain', required=False)()
    scrapying_start_time_from = DateTimeParameter(
        'scrapying_start_time_from', required=False,)
    scrapying_start_time_to = DateTimeParameter(
        'scrapying_start_time_to', required=False)
    task = ScrapedNewsClipMasterSaveTask(
        log_file_path=LOG_FILE_PATH, start_time=datetime.now().astimezone(TIMEZONE))
    result = task(domain=domain, scrapying_start_time_from=scrapying_start_time_from,
                  scrapying_start_time_to=scrapying_start_time_to)
