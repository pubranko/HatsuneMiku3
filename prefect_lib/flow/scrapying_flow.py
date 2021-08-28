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
from prefect_lib.task.scrapying_task import ScrapyingTask
from prefect_lib.settings import TIMEZONE
from prefect.utilities.context import Context
from common_lib.mail_send import mail_send


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
    name='Scrapying flow',
    state_handlers=[status_change],
) as flow:

    domain = Parameter('domain', required=False)()
    response_time_from = DateTimeParameter(
        'response_time_from', required=False,)
    response_time_to = DateTimeParameter(
        'response_time_to', required=False)

    task = ScrapyingTask(
        log_file_path=log_file_path, starting_time=starting_time)
    result = task(domain=domain, response_time_from=response_time_from,
                  response_time_to=response_time_to)

# domain、response_time_*による絞り込みは任意
flow.run(parameters=dict(
    # domain='epochtimes.jp',
    #response_time_from=datetime(2021, 8, 14, 0, 0, 0).astimezone(TIMEZONE),
    #response_time_to=datetime(2021, 8, 14, 23, 19, 53).astimezone(TIMEZONE),
))
