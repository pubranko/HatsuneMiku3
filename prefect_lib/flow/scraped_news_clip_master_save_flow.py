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
from prefect_lib.task.scraped_news_clip_master_save_task import ScrapedNewsClipMasterSaveTask
from prefect_lib.settings import TIMEZONE
from prefect_lib.common_module.flow_status_change import flow_status_change

start_time = datetime.now().astimezone(
    TIMEZONE)
log_file_path = os.path.join(
    'logs', os.path.splitext(os.path.basename(__file__))[0] + '.log')
logging.basicConfig(level=logging.DEBUG, filemode="w+", filename=log_file_path,
                    format='%(asctime)s %(levelname)s [%(name)s] : %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

with Flow(
    name='Scraped news clip master save flow',
    state_handlers=[flow_status_change],
) as flow:
    domain = Parameter('domain', required=False)()
    scrapying_start_time_from = DateTimeParameter(
        'scrapying_start_time_from', required=False,)
    scrapying_start_time_to = DateTimeParameter(
        'scrapying_start_time_to', required=False)
    task = ScrapedNewsClipMasterSaveTask(
        log_file_path=log_file_path, start_time=start_time)
    result = task(domain=domain, scrapying_start_time_from=scrapying_start_time_from,
                  scrapying_start_time_to=scrapying_start_time_to)

# domain、scrapying_start_time_*による絞り込みは任意
flow.run(parameters=dict(
    #domain='kyodo.co.jp',
    #domain='epochtimes.jp',
    #scrapying_start_time_from=datetime(2021, 8, 21, 0, 0, 0).astimezone(TIMEZONE),
    #scrapying_start_time_to=datetime(2021, 8, 21, 10, 18, 12, 160000).astimezone(TIMEZONE),
    #scrapying_start_time_from=datetime(2021, 9, 25, 15, 26, 37, 344148).astimezone(TIMEZONE),
    #scrapying_start_time_to=datetime(2021, 9, 25, 15, 26, 37, 344148).astimezone(TIMEZONE),
))