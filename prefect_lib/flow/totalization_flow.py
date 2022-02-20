import os
import sys
from datetime import datetime
from prefect import Flow, task, Parameter
from prefect.core.parameter import DateTimeParameter
from prefect.tasks.control_flow.conditional import ifelse
from prefect.engine import signals
from prefect.utilities.context import Context
path = os.getcwd()
sys.path.append(path)
from prefect_lib.settings import TIMEZONE
from prefect_lib.common_module.logging_setting import log_file_path
from prefect_lib.common_module.flow_status_change import flow_status_change
from prefect_lib.task.totalization_task import TotalizationTask

'''
基準日(base_date)のログより集計を行う。
※基準日の指定がない場合、前日を基準日とする。
'''
with Flow(
    name='Totalization flow',
    state_handlers=[flow_status_change],
) as flow:
    base_date = DateTimeParameter('base_date', required=False,)
    task = TotalizationTask(
        log_file_path=log_file_path, start_time=datetime.now().astimezone(TIMEZONE))
    result = task(base_date=base_date,)

flow.run(parameters=dict(
    base_date=datetime(2022, 2, 11).astimezone(TIMEZONE),
))
