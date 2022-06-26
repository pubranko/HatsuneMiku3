import os
import sys
from datetime import datetime
from prefect.core.flow import Flow
from prefect.core.parameter import Parameter
from prefect.tasks.control_flow.conditional import ifelse
from prefect.utilities.context import Context
path = os.getcwd()
sys.path.append(path)
from prefect_lib.settings import TIMEZONE
from prefect_lib.common_module.logging_setting import LOG_FILE_PATH
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
    in_out = Parameter('in_out', required=True)()  # in:登録、out：削除

    task = RegularObservationControllerUpdateTask(
        log_file_path=LOG_FILE_PATH, start_time=datetime.now().astimezone(TIMEZONE))
    result = task(spiders_name=spiders_name, in_out=in_out)
