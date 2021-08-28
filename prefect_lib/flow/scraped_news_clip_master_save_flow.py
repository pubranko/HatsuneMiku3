import os
import sys
from datetime import datetime
import logging
path = os.getcwd()
sys.path.append(path)
import prefect
from prefect import Flow, task, Parameter
from prefect.core.parameter import DateTimeParameter
from prefect.tasks.control_flow.conditional import ifelse
from prefect.engine import signals
# ステータス一覧： Running,Success,Failed,Cancelled,TimedOut,TriggerFailed,ValidationFailed,Skipped,Mapped,Cached,Looped,Finished,Cancelling,Retrying,Resume,Queued,Submitted,ClientFailed,Paused,Scheduled,Pending
from prefect.engine.state import Running, Success, Failed
from prefect_lib.task.scraped_news_clip_master_save_task import ScrapedNewsClipMasterSaveTask
from prefect.utilities.context import Context
from common_lib.mail_send import mail_send
from prefect_lib.settings import TIMEZONE


starting_time = datetime.now().astimezone(
    TIMEZONE)
log_file_path = os.path.join(
    'logs', os.path.splitext(os.path.basename(__file__))[0] + '.log')
logging.basicConfig(level=logging.DEBUG, filemode="w+", filename=log_file_path,
                    format='%(asctime)s %(levelname)s [%(name)s] : %(message)s', datefmt='%Y-%m-%d %H:%M:%S')


def status_change(obj: Flow, old_state, new_state):
    '''フローのステータスが変更される際に必要な処理を行う'''
    if isinstance(new_state, Running):
        pass  # フロー開始時
    elif isinstance(new_state, Success):
        pass  # 正常終了時の処理
    elif isinstance(new_state, Failed):
        # logファイルをメールで送信
        with open(log_file_path) as f:
            log_file = f.read()
        # mail_send('【prefectフローでエラー発生】' +
        #           crawl_start_time.isoformat(), log_file,)

    if not isinstance(new_state, Running):
        pass  # 成否に関係なく終わったときに動く処理


with Flow(
    name='Scraped news clip master save flow',
    state_handlers=[status_change],
) as flow:

    domain = Parameter('domain', required=False)()
    scrapying_starting_time_from = DateTimeParameter(
        'scrapying_starting_time_from', required=False,)
    scrapying_starting_time_to = DateTimeParameter(
        'scrapying_starting_time_to', required=False)

    task = ScrapedNewsClipMasterSaveTask(
        log_file_path=log_file_path, starting_time=starting_time)
    result = task(domain=domain, scrapying_starting_time_from=scrapying_starting_time_from,
                  scrapying_starting_time_to=scrapying_starting_time_to)

# flow.run()
# domain、scrapying_starting_time_*による絞り込みは任意
flow.run(parameters=dict(
    domain='',
    #domain='epochtimes.jp',
    #scrapying_starting_time_from=datetime(2021, 8, 21, 0, 0, 0).astimezone(TIMEZONE),
    #scrapying_starting_time_to=datetime(2021, 8, 21, 10, 18, 12, 160000).astimezone(TIMEZONE),
    #scrapying_starting_time_from=datetime(2021, 8, 21, 10, 18, 12, 161000).astimezone(TIMEZONE),
    #scrapying_starting_time_to=datetime(2021, 8, 21, 10, 18, 12, 160000).astimezone(TIMEZONE),
))

#2021-08-21T01:18:12.160Z
#2021-08-21T05:12:46.400Z
#2021-08-21T05:13:11.809Z
