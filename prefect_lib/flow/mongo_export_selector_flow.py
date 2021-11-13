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
from prefect_lib.task.mongo_export_selector_task import MongoExportSelectorTask
from prefect_lib.settings import TIMEZONE
from prefect_lib.common_module.flow_status_change import flow_status_change

start_time = datetime.now().astimezone(TIMEZONE)
log_file_path = os.path.join(
    'logs', os.path.splitext(os.path.basename(__file__))[0] + '.log')
logging.basicConfig(level=logging.DEBUG, filemode="w+", filename=log_file_path,
                    format='%(asctime)s %(levelname)s [%(name)s] : %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

with Flow(
    name='Mongo export selector flow',
    state_handlers=[flow_status_change],
) as flow:
    collections_name = Parameter(
        'collections_name', required=True,
        default=[
            'asynchronous_report', 'controller', 'crawler_logs', 'crawler_response', 'news_clip_master'
        ])
    base_month = Parameter('base_month', required=True,)
    backup_dir = Parameter('backup_dir', required=True,)
    task = MongoExportSelectorTask(
        log_file_path=log_file_path, start_time=start_time)
    result = task(collections_name=collections_name,
                  base_month=base_month, backup_dir=backup_dir,)

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
    base_month='2021-11',  # エクスポートを行うデータの基準年月
    backup_dir='2021-11test1',   # prefect_lib.settings.BACKUP_BASE_DIR内のディレクトリを指定
    # backup_dir=base_month,
))
