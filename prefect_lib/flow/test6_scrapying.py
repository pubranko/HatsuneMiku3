import os
import sys
from datetime import datetime
import logging
from logging import Logger
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
from prefect.utilities.context import Context
from common.mail_send import mail_send
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
    name='Scrapying Test Flow',
    state_handlers=[status_change],
) as flow:

    module = Parameter(
        'module', default='prefect_lib.run.test_scrapying', required=True)()
    method = Parameter('method', default='test1', required=True)()
    domain = Parameter('domain', required=False)()
    response_time_from = DateTimeParameter(
        'response_time_from', required=False,)
    response_time_to = DateTimeParameter(
        'response_time_to', required=False)

    task = ScrapyingTask(
        log_file_path=log_file_path, starting_time=starting_time)
    result = task(module=module, method=method, domain=domain,
                  response_time_from=response_time_from, response_time_to=response_time_to)

# flow.run()
### domain、response_time_*による絞り込みは任意
flow.run(parameters=dict(
    module='prefect_lib.run.test_scrapying',
    method='test1',
    #domain='',
    #response_time_from=datetime(2021, 8, 14, 0, 0, 0).astimezone(TIMEZONE),
    #response_time_to=datetime(2021, 8, 14, 23, 19, 53).astimezone(TIMEZONE),
))
    #response_time_from='2021-08-01 00:00:00+09:00',
    #response_time_to='2021-08-10 00:00:00+09:00',


'''
【残課題】


【後続の作業】
・スクレイピング(名前未定)
　　※ログのファイル名切り替え随時
・solrへ流し込み(名前未定)
　　※ログのファイル名切り替え随時

【日次レポート機能】
【週次レポート機能】
【月次レポート機能】
【年次レポート機能】
・各サイト別、全サイト単位件数
・クロール、件数バイト数
・正常、件数
・エラー、件数
・平均時間
・最長時間
・最大件数
・最大バイト数


【別プロジェクトとして管理するべき。別途検討】
・API郡(twitter、FBなど)クロール（起動しっぱなし？）
※ログのファイル名切り替え随時
・solrへ流し込み
※ログのファイル名切り替え随時

'''
