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
from prefect_lib.task.mongo_export_selector_task import MongoExportSelector
from prefect_lib.settings import TIMEZONE
from prefect_lib.common_module.flow_status_change import flow_status_change

start_time = datetime.now().astimezone(TIMEZONE)
log_file_path = os.path.join(
    'logs', os.path.splitext(os.path.basename(__file__))[0] + '.log')
logging.basicConfig(level=logging.DEBUG, filemode="w+", filename=log_file_path,
                    format='%(asctime)s %(levelname)s [%(name)s] : %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

with Flow(
    name='Mongo export selector flow',
    state_handlers=[flow_status_change],
) as flow:
    collections = Parameter(
        'collections', required=True,)
    backup_yyyymm = Parameter(
        'backup_yyyymm', required=True,)
    task = MongoExportSelector(
        log_file_path=log_file_path, start_time=start_time)
    result = task(collections=collections,backup_yyyymm=backup_yyyymm)

flow.run(parameters=dict(
    collections=[],
    backup_yyyymm='2021-10',
))