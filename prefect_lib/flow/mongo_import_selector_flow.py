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
from prefect_lib.task.mongo_import_selector_task import MongoImportSelectorTask
from prefect_lib.settings import TIMEZONE
from prefect_lib.common_module.flow_status_change import flow_status_change

start_time = datetime.now().astimezone(TIMEZONE)
log_file_path = os.path.join(
    'logs', os.path.splitext(os.path.basename(__file__))[0] + '.log')
logging.basicConfig(level=logging.DEBUG, filemode="w+", filename=log_file_path,
                    format='%(asctime)s %(levelname)s [%(name)s] : %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

with Flow(
    name='Mongo import selector flow',
    state_handlers=[flow_status_change],
) as flow:
    collections_name = Parameter(
        'collections_name', required=True,)
    time_stamp_from = DateTimeParameter(
        'time_stamp_from', required=False,)
    time_stamp_to = DateTimeParameter(
        'time_stamp_to', required=False,)

    task = MongoImportSelectorTask(
        log_file_path=log_file_path, start_time=start_time)
    result = task(collections_name=collections_name,
                  time_stamp_from=time_stamp_from, time_stamp_to=time_stamp_to)

flow.run(parameters=dict(
    collections_name=[
        'crawler_response',
        'scraped_from_response',
        'news_clip_master',
        'crawler_logs',
        'asynchronous_report',
        'controller',
    ],
    time_stamp_from=datetime(2021, 11, 3, 0, 0, 0).astimezone(TIMEZONE),
    time_stamp_to=datetime(2021, 11, 3, 23, 59, 59).astimezone(TIMEZONE),
))
