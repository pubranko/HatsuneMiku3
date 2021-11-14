import os
import sys
import logging
from datetime import datetime
from prefect import Flow, task, Parameter
from prefect.tasks.control_flow.conditional import ifelse
from prefect.engine import signals
from prefect.utilities.context import Context
path = os.getcwd()
sys.path.append(path)
from prefect_lib.task.mongo_import_selector_task import MongoImportSelectorTask
from prefect_lib.settings import TIMEZONE
from prefect_lib.common_module.flow_status_change import flow_status_change

start_time = datetime.now().astimezone(TIMEZONE)
log_file_path = os.path.join(
    'logs', os.path.splitext(os.path.basename(__file__))[0] + '.log')
logging.basicConfig(level=logging.DEBUG, filemode="w+", filename=log_file_path,
                    format='%(asctime)s %(levelname)s [%(name)s] : %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

with Flow(
    name='Mongo import selector flow',
    state_handlers=[flow_status_change],
) as flow:
    collections_name = Parameter(
        'collections_name', required=True,)
    backup_dir_from = Parameter(
        'backup_dir_from', required=True,)
    backup_dir_to = Parameter(
        'backup_dir_to', required=True,)
    task = MongoImportSelectorTask(
        log_file_path=log_file_path, start_time=start_time)
    result = task(collections_name=collections_name,
                  backup_dir_from=backup_dir_from,
                  backup_dir_to=backup_dir_to)

flow.run(parameters=dict(
    collections_name=[
        # 'crawler_response',
        # 'scraped_from_response',
        # 'news_clip_master',
        # 'crawler_logs',
        # 'asynchronous_report',
        # 'controller',
    ],
    backup_dir_from='2021-11',    # prefect_lib.settings.BACKUP_BASE_DIR内のディレクトリを指定
    backup_dir_to='2021-11',    # prefect_lib.settings.BACKUP_BASE_DIR内のディレクトリを指定
))
