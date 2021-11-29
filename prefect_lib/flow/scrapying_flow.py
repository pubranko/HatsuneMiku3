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
from prefect_lib.task.scrapying_task import ScrapyingTask
from prefect_lib.settings import TIMEZONE
from prefect_lib.common_module.flow_status_change import flow_status_change

start_time = datetime.now().astimezone(TIMEZONE)
log_file_path = os.path.join(
    'logs', os.path.splitext(os.path.basename(__file__))[0] + '.log')
logging.basicConfig(level=logging.DEBUG, filemode="w+", filename=log_file_path,
                    format='%(asctime)s %(levelname)s [%(name)s] : %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

with Flow(
    name='Scrapying flow',
    state_handlers=[flow_status_change],
) as flow:
    domain = Parameter('domain', required=False)()
    crawling_start_time_from = DateTimeParameter(
        'crawling_start_time_from', required=False,)
    crawling_start_time_to = DateTimeParameter(
        'crawling_start_time_to', required=False)
    urls = Parameter('urls', required=False)()
    following_processing_execution = Parameter(
        'following_processing_execution', default='No')()
    task = ScrapyingTask(
        log_file_path=log_file_path, start_time=start_time)
    result = task(domain=domain,
                  crawling_start_time_from=crawling_start_time_from,
                  crawling_start_time_to=crawling_start_time_to,
                  urls=urls,
                  following_processing_execution=following_processing_execution,)

# domain、crawling_start_time_*による絞り込みは任意
flow.run(parameters=dict(
    domain='epochtimes.jp',
    crawling_start_time_from=datetime(2021, 11, 29, 20, 0, 0, 0).astimezone(TIMEZONE),
    #crawling_start_time_to=datetime(2021, 9, 25, 11, 8, 28, 286000).astimezone(TIMEZONE),
    #urls=['https://www.sankei.com/article/20210829-2QFVABFPMVIBNHSINK6TBYWEXE/?outputType=theme_tokyo2020',]
    following_processing_execution='Yes',    # 後続処理実行(news_clip_masterへの登録,solrへの登録)
))