import os
import sys
from datetime import datetime
from prefect.core.flow import Flow
from prefect.core.parameter import Parameter
from prefect.tasks.control_flow.conditional import ifelse
from prefect.utilities.context import Context
path = os.getcwd()
sys.path.append(path)
from prefect_lib.common_module.flow_status_change import flow_status_change
from prefect_lib.task.regular_observation_controller_update_task import RegularObservationControllerUpdateTask

'''
定期観測対象のスパイダーの登録・削除を行う。
'''
with Flow(
    name='[ENTRY_002] Regular observation controller update Flow',
    state_handlers=[flow_status_change],
) as flow:

    spiders_name = Parameter(
        'spiders_name', required=True)()  # 登録・削除したいドメインを指定
    register = Parameter('register', required=True)()  # add:登録、delete：削除

    task = RegularObservationControllerUpdateTask()
    result = task(spiders_name=spiders_name, register=register)
