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
from prefect_lib.task.mongo_import_selector_task import MongoImportSelector
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
    collections = Parameter(
        'collections', required=True,)
    from_when = DateTimeParameter(
        'from_when', required=False,)
    to_when = DateTimeParameter(
        'to_when', required=False,)

    task = MongoImportSelector(
        log_file_path=log_file_path, start_time=start_time)
    result = task(collections=collections,
                  from_when=from_when, to_when=to_when)

flow.run(parameters=dict(
    collections=[],
    # collections=['crawler_response'],
    # collections=['a'],
    #from_when=datetime(2021, 10, 28, 22, 47, 52).astimezone(TIMEZONE),
    #to_when=datetime(2021, 10, 28, 22, 46, 55).astimezone(TIMEZONE),
))
