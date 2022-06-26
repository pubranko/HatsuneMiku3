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
from prefect_lib.task.solr_news_clip_save_task import SolrNewsClipSaveTask

'''
news_clipへの登録を実行できる。
・対象のドメイン、スクレイピング時間を指定できる。
'''
with Flow(
    name='[CRAWL_007] Solr news clip save flow',
    state_handlers=[flow_status_change],
) as flow:

    domain = Parameter('domain', required=False)()
    scraped_save_start_time_from = DateTimeParameter(
        'scraped_save_start_time_from', required=False,)
    scraped_save_start_time_to = DateTimeParameter(
        'scraped_save_start_time_to', required=False)

    task = SolrNewsClipSaveTask(
        log_file_path=LOG_FILE_PATH, start_time=datetime.now().astimezone(TIMEZONE))
    result = task(domain=domain, scraped_save_start_time_from=scraped_save_start_time_from,
                  scraped_save_start_time_to=scraped_save_start_time_to)
