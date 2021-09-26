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
from prefect_lib.task.stop_controller_update_task import StopControllerUpdateTask
from prefect_lib.settings import TIMEZONE
from prefect.utilities.context import Context
from common_lib.mail_send import mail_send


start_time = datetime.now().astimezone(
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
        #           crawling_start_time.isoformat(), log_file,)

    if not isinstance(new_state, Running):
        pass  # 成否に関係なく終わったときに動く処理


with Flow(
    name='Stop Controller Update Flow',
    state_handlers=[status_change],
) as flow:

    domain = Parameter('domain', required=True)()   #登録・削除したいドメインを指定
    in_out = Parameter('in_out', required=True)()   #in:登録、out：削除
    destination = Parameter('destination', required=True)()   #crawlingとscrapyingの選択

    task = StopControllerUpdateTask(
        log_file_path=log_file_path, start_time=start_time)
    result = task(domain=domain, in_out=in_out,
                  destination=destination)

# domain、crawling_start_time_*による絞り込みは任意
flow.run(parameters=dict(
    # domain='epochtimes.jp',
    # in_out='in',
    # destination='crawling',
    # domain='epochtimes.jp',
    # in_out='out',
    # destination='crawling',
    # domain='sankei.com',
    # in_out='in',
    # destination='scrapying',
    domain='sankei.com',
    in_out='out',
    destination='scrapying',
))