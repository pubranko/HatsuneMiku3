import os
import sys
from datetime import datetime
import logging
from prefect import Flow, task, Parameter
from prefect.tasks.control_flow.conditional import ifelse
from prefect.engine import signals
from prefect.utilities.context import Context
path = os.getcwd()
sys.path.append(path)
from prefect_lib.task.stop_controller_update_task import StopControllerUpdateTask
from prefect_lib.settings import TIMEZONE
from prefect_lib.common_module.flow_status_change import flow_status_change

start_time = datetime.now().astimezone(TIMEZONE)
log_file_path = os.path.join(
    'logs', os.path.splitext(os.path.basename(__file__))[0] + '.log')
logging.basicConfig(level=logging.DEBUG, filemode="w+", filename=log_file_path,
                    format='%(asctime)s %(levelname)s [%(name)s] : %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

with Flow(
    name='Stop Controller Update Flow',
    state_handlers=[flow_status_change],
) as flow:
    domain = Parameter('domain', required=True)()   #登録・削除したいドメインを指定
    in_out = Parameter('in_out', required=True)()   #in:登録、out：削除
    destination = Parameter('destination', required=True)()   #crawlingとscrapyingの選択
    task = StopControllerUpdateTask(
        log_file_path=log_file_path, start_time=start_time)
    result = task(domain=domain, in_out=in_out,
                  destination=destination)

# domain、crawling_start_time_*による絞り込みは任意
flow.run(parameters=dict(
    # domain='epochtimes.jp',
    # in_out='in',
    # destination='crawling',
    # domain='epochtimes.jp',
    # in_out='out',
    # destination='crawling',
    # domain='sankei.com',
    # in_out='in',
    # destination='scrapying',
    domain='sankei.com',
    in_out='out',
    destination='scrapying',
))
