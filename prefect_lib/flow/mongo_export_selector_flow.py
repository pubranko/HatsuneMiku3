import os
import sys
from datetime import datetime
from prefect import Flow, task, Parameter
from prefect.tasks.control_flow.conditional import ifelse
from prefect.engine import signals
from prefect.utilities.context import Context
path = os.getcwd()
sys.path.append(path)
from prefect_lib.settings import TIMEZONE
from prefect_lib.common_module.logging_setting import log_file_path
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
    name='Mongo export selector flow',
    state_handlers=[flow_status_change],
) as flow:
    collections_name = Parameter(
        'collections_name', required=True,
        default=[
            'asynchronous_report', 'controller', 'crawler_logs', 'crawler_response', 'news_clip_master'
        ])
    export_dir_extended_name = Parameter('export_dir_extended_name', required=False,)

    export_period_from = Parameter('export_period_from', required=False,)
    export_period_to = Parameter('export_period_to', required=False,)

    crawler_response__registered = Parameter(
        'crawler_response__registered', required=False,)
    task = MongoExportSelectorTask(
        log_file_path=log_file_path, start_time=datetime.now().astimezone(TIMEZONE))
    result = task(collections_name=collections_name,
                  export_period_from=export_period_from,
                  export_period_to=export_period_to,
                  export_dir_extended_name=export_dir_extended_name,
                  crawler_response__registered=crawler_response__registered,
                  )

flow.run(parameters=dict(
    # collections=['asynchronous_report','controller','crawler_logs','crawler_response','news_clip_master','scraped_from_response'],
    collections_name=[
        'crawler_response',
        'scraped_from_response',  # 通常運用では不要なバックアップとなるがテスト用に実装している。
        'news_clip_master',
        'crawler_logs',
        'asynchronous_report',
        'controller',
    ],
    export_dir_extended_name='test4',   # export先のフォルダyyyy-mmの末尾に拡張した名前を付与する(testや臨時でエクスポートしたい時などに使う)
    export_period_from='2022-02',  # 月次エクスポートを行うデータの基準年月
    export_period_to='2022-02',  # 月次エクスポートを行うデータの基準年月
    crawler_response__registered=True,   # crawler_responseの場合、登録済みになったレコードのみエクスポートする場合True、登録済み以外のレコードも含めてエクスポートする場合False
))
