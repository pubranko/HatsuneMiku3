import os
import sys
from datetime import datetime
import logging
from logging import Logger
path = os.getcwd()
sys.path.append(path)
import prefect
from prefect import Flow, task
from prefect.tasks.control_flow.conditional import ifelse
from prefect.engine import signals
# ステータス一覧： Running,Success,Failed,Cancelled,TimedOut,TriggerFailed,ValidationFailed,Skipped,Mapped,Cached,Looped,Finished,Cancelling,Retrying,Resume,Queued,Submitted,ClientFailed,Paused,Scheduled,Pending
from prefect.engine.state import Running, Success, Failed
from prefect_flow.task.regular_observation_task import RegularObservationTask
from prefect.utilities.context import Context
from common.mail_send import mail_send
from prefect_flow.common_module.timezone import TIMEZONE


crawl_start_time = datetime.now().astimezone(
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
        mail_send('【prefectフローでエラー発生】' +
                  crawl_start_time.isoformat(), log_file,)

    if not isinstance(new_state, Running):
        pass  # 成否に関係なく終わったときに動く処理


with Flow(
    name='RegularObservation',
    state_handlers=[status_change],
) as flow:

    prefect_context:Context = prefect.context
    task = RegularObservationTask(
        log_file_path=log_file_path, crawl_start_time=crawl_start_time, notice_level='CRITICAL')
        #log_file_path=log_file_path, crawl_start_time=crawl_start_time, notice_level='ERROR')
        #log_file_path=log_file_path, crawl_start_time=crawl_start_time, notice_level='WARNING')
    result = task()

flow.run()

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
