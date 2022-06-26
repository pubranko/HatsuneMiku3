import os
import sys
from datetime import datetime
from prefect.core.flow import Flow
from prefect.tasks.control_flow.conditional import ifelse
from prefect.engine import signals
from prefect.utilities.context import Context
path = os.getcwd()
sys.path.append(path)
from prefect_lib.settings import TIMEZONE
from prefect_lib.common_module.logging_setting import LOG_FILE_PATH
from prefect_lib.common_module.flow_status_change import flow_status_change
from prefect_lib.task.regular_observation_task import RegularObservationTask

'''
定期観測を行う。
'''
with Flow(
    name='[CRAWL_002] Regular observation flow',
    state_handlers=[flow_status_change],
) as flow:
    task = RegularObservationTask(
        log_file_path=LOG_FILE_PATH, start_time=datetime.now().astimezone(TIMEZONE))
    result = task()
