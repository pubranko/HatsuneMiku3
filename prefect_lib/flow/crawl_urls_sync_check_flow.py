import os
import sys
import logging
from datetime import datetime
from prefect import Flow, task, Parameter
from prefect.core.parameter import DateTimeParameter
from prefect.tasks.control_flow.conditional import ifelse
from prefect.utilities.context import Context
from prefect.engine import signals
path = os.getcwd()
sys.path.append(path)
from prefect_lib.task.crawl_urls_sync_check_task import CrawlUrlsSyncCheckTask
from prefect_lib.settings import TIMEZONE
from prefect_lib.common_module.flow_status_change import flow_status_change

start_time = datetime.now().astimezone(
    TIMEZONE)
log_file_path = os.path.join(
    'logs', os.path.splitext(os.path.basename(__file__))[0] + '.log')
logging.basicConfig(level=logging.DEBUG, filemode="w+", filename=log_file_path,
                    format='%(asctime)s %(levelname)s [%(name)s] : %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

with Flow(
    name='Crawl urls sync check flow',
    #state_handlers=[status_change],
    state_handlers=[flow_status_change],
) as flow:
    domain = Parameter('domain', required=False)()
    start_time_from = DateTimeParameter(
        'start_time_from', required=False,)
    start_time_to = DateTimeParameter(
        'start_time_to', required=False)
    task = CrawlUrlsSyncCheckTask(
        log_file_path=log_file_path, start_time=start_time)
    result = task(domain=domain,
                  start_time_from=start_time_from,
                  start_time_to=start_time_to)

# domain、start_time_*による絞り込みは任意
flow.run(parameters=dict(
    #domain='sankei.com',
    #start_time_from=datetime(2021, 9, 25, 11, 8, 28, 286000).astimezone(TIMEZONE),
    #start_time_to=datetime(2021, 9, 25, 11, 8, 28, 286000).astimezone(TIMEZONE),
    #urls=['https://www.sankei.com/article/20210829-2QFVABFPMVIBNHSINK6TBYWEXE/?outputType=theme_tokyo2020',]
))
#2021-08-29T13:33:48.503Z