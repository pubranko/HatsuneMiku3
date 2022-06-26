import os
import sys
from datetime import datetime
from prefect.core.flow import Flow
from prefect.core.parameter import Parameter
from prefect.tasks.control_flow.conditional import ifelse
from prefect.engine import signals
from prefect.utilities.context import Context
path = os.getcwd()
sys.path.append(path)
from prefect_lib.common_module.flow_status_change import flow_status_change
from prefect_lib.task.stop_controller_update_task import StopControllerUpdateTask

'''
クロール対象のドメインの登録・削除を行う。
スクレイピング対象のドメインの登録・削除を行う。
'''
with Flow(
    name='[ENTRY_003] Stop Controller Update Flow',
    state_handlers=[flow_status_change],
) as flow:
    domain = Parameter('domain', required=True)()   #登録・削除したいドメインを指定
    in_out = Parameter('in_out', required=True)()   #in:登録、out：削除
    destination = Parameter('destination', required=True)()   #crawlingとscrapyingの選択
    task = StopControllerUpdateTask()
    result = task(domain=domain, in_out=in_out,
                  destination=destination)
