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
from prefect_lib.settings import TIMEZONE
from prefect_lib.common_module.logging_setting import LOG_FILE_PATH
from prefect_lib.common_module.flow_status_change import flow_status_change
from prefect_lib.task.mongo_export_selector_task import MongoExportSelectorTask

'''
mongoDBのエクスポートを行う。
・mongoDBから取得したコレクションをpythonのlistにし、pickle.dumpsでシリアライズしてエクスポートする。
・対象のコレクションを選択できる。
・対象の年月を指定できる。範囲を指定した場合、月ごとにエクスポートを行う。
・crawler_responseの場合、登録済みになったレコードのみとするか全てとするか選択できる。
'''
with Flow(
    name='[MONGO_003] Mongo export selector flow',
    state_handlers=[flow_status_change],
) as flow:
    collections_name = Parameter(
        'collections_name', required=True,
        default=[
            'asynchronous_report', 'controller', 'crawler_logs', 'crawler_response', 'news_clip_master'
        ])
    prefix = Parameter('prefix', required=False,)

    export_period_from = Parameter('export_period_from', required=False,)
    export_period_to = Parameter('export_period_to', required=False,)

    crawler_response__registered = Parameter(
        'crawler_response__registered', required=False,)
    task = MongoExportSelectorTask(
        log_file_path=LOG_FILE_PATH, start_time=datetime.now().astimezone(TIMEZONE))
    result = task(collections_name=collections_name,
                  export_period_from=export_period_from,
                  export_period_to=export_period_to,
                  prefix=prefix,
                  crawler_response__registered=crawler_response__registered,
                  )
