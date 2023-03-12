import os
import sys
from datetime import datetime
from prefect.core.flow import Flow
from prefect.core.parameter import Parameter
from prefect.core.parameter import DateTimeParameter
from prefect.tasks.control_flow.conditional import ifelse
from prefect.engine import signals
from prefect.utilities.context import Context
path = os.getcwd()
sys.path.append(path)
from prefect_lib.common_module.flow_status_change import flow_status_change
from prefect_lib.task.scrapying_task import ScrapyingTask

'''
スクレイピングを実行できる。
・対象のドメイン、クロール時間を指定できる。
・後続のnews_clip_masterへの登録〜news_clipへの登録まで一括で実行するか選択できる。
'''
with Flow(
    name='[CRAWL_004] Scrapying flow',
    state_handlers=[flow_status_change],
) as flow:
    domain = Parameter('domain', required=False)()
    target_start_time_from = DateTimeParameter(
        'target_start_time_from', required=False,)
    target_start_time_to = DateTimeParameter(
        'target_start_time_to', required=False)
    urls = Parameter('urls', required=False)()
    following_processing_execution = Parameter(
        'following_processing_execution', default='No')()
    task = ScrapyingTask()
    result = task(domain=domain,
                  target_start_time_from=target_start_time_from,
                  target_start_time_to=target_start_time_to,
                  urls=urls,
                  following_processing_execution=following_processing_execution,)
