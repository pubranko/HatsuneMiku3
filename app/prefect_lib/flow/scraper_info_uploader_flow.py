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
from prefect_lib.task.scraper_info_uploader_task import ScraperInfoUploaderTask

'''
mongoDBのインポートを行う。
・pythonのlistをpickle.loadsで復元しインポートする。
・対象のコレクションを選択できる。
・対象の年月を指定できる。範囲を指定した場合、月ごとにエクスポートを行う。
'''
with Flow(
    name='[ENTRY_001] Scraper info uploader flow',
    state_handlers=[flow_status_change],
) as flow:
    scraper_info_by_domain_files = Parameter(
        'scraper_info_by_domain_files', required=True,)
    task = ScraperInfoUploaderTask(
        log_file_path=LOG_FILE_PATH, start_time=datetime.now().astimezone(TIMEZONE))
    result = task(scraper_info_by_domain_files=scraper_info_by_domain_files)
