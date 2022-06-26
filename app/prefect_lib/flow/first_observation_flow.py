import os
import sys
from datetime import datetime
from prefect.core.flow import Flow
from prefect.tasks.control_flow.conditional import ifelse
from prefect.engine import signals
from prefect.utilities.context import Context
path = os.getcwd()
sys.path.append(path)
from prefect_lib.common_module.flow_status_change import flow_status_change
from prefect_lib.task.first_observation_task import FirstObservationTask

'''
定期観測クロールのため、以下の条件を満たすスパイダーに対して初回クロールの実行を行う。
①定期観測コントローラーに登録のあるスパイダー
②前回クロール情報がないスパイダー
'''
with Flow(
    name='[CRAWL_001] First observation flow',
    state_handlers=[flow_status_change],
) as flow:

    task = FirstObservationTask()
    result = task()
