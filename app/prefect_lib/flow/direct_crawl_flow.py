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
from prefect_lib.task.direct_crawl_task import DirectCrawlTask

'''
ダイレクトクロール
・指定したurlへのみクロールを行う。
・事前に"prefect_lib.settings.DIRECT_CRAWL_FILES_DIR"配下のファイルにクロールしたいurlを登録する。
・実行時にそのファイル名を指定する。
'''
with Flow(
    name='[CRAWL_005] Direct crawl flow',
    state_handlers=[flow_status_change],
) as flow:

    spider_name = Parameter('spider_name', required=True)()
    file = Parameter('file', required=False)()
    task = DirectCrawlTask(
        log_file_path=LOG_FILE_PATH, start_time=datetime.now().astimezone(TIMEZONE))
    result = task(spider_name=spider_name, file=file,)
