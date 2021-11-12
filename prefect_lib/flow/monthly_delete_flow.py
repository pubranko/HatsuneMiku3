import os
import sys
import logging
from datetime import datetime
from prefect import Flow, task, Parameter
from prefect.core.parameter import DateTimeParameter
from prefect.tasks.control_flow.conditional import ifelse
from prefect.engine import signals
from prefect.utilities.context import Context
path = os.getcwd()
sys.path.append(path)
from prefect_lib.task.monthly_delete_task import MonthlyDeleteTask
from prefect_lib.settings import TIMEZONE
from prefect_lib.common_module.flow_status_change import flow_status_change

start_time = datetime.now().astimezone(TIMEZONE)
log_file_path = os.path.join(
    'logs', os.path.splitext(os.path.basename(__file__))[0] + '.log')
logging.basicConfig(level=logging.DEBUG, filemode="w+", filename=log_file_path,
                    format='%(asctime)s %(levelname)s [%(name)s] : %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

with Flow(
    name='Monthly delete flow',
    state_handlers=[flow_status_change],
) as flow:
    collections_name = Parameter(
        'collections_name', required=True,
        default=[
            'asynchronous_report', 'crawler_logs', 'crawler_response',
        ])
    number_of_months_elapsed = Parameter(
        'number_of_months_elapsed', required=True, default=3)
    task = MonthlyDeleteTask(
        log_file_path=log_file_path, start_time=start_time)
    result = task(collections_name=collections_name, number_of_months_elapsed=number_of_months_elapsed)

flow.run(parameters=dict(
    collections_name=[
        'crawler_response',
        # 'crawler_logs',
        # 'asynchronous_report',
    ],
    number_of_months_elapsed=0,
))
