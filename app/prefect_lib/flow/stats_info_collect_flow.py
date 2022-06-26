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
from prefect_lib.task.stats_info_collect_task import StatsInfoCollectTask

'''
基準日(base_date)のログより集計を行う。
※基準日の指定がない場合、前日を基準日とする。
'''
with Flow(
    name='[STATS_001] Stats info collect flow',
    state_handlers=[flow_status_change],
) as flow:
    base_date = DateTimeParameter('base_date', required=False,)
    task = StatsInfoCollectTask(
        log_file_path=LOG_FILE_PATH, start_time=datetime.now().astimezone(TIMEZONE))
    result = task(base_date=base_date,)
