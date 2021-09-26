import os
import sys
from datetime import datetime
import logging
from logging import Logger
path = os.getcwd()
sys.path.append(path)
import prefect
from prefect import Flow, task, Parameter
from prefect.tasks.control_flow.conditional import ifelse
from prefect.engine import signals
# ステータス一覧： Running,Success,Failed,Cancelled,TimedOut,TriggerFailed,ValidationFailed,Skipped,Mapped,Cached,Looped,Finished,Cancelling,Retrying,Resume,Queued,Submitted,ClientFailed,Paused,Scheduled,Pending
from prefect.engine.state import Running, Success, Failed
from prefect_lib.task.scrapy_crawling_task import ScrapyCrawlingTask
from prefect.utilities.context import Context
from common_lib.mail_send import mail_send
from prefect_lib.settings import TIMEZONE


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
    name='Scrapy crawling flow',
    state_handlers=[status_change],
) as flow:
    spider_names = Parameter('spider_names')()
    spider_kwargs = Parameter('spider_kwargs')()
    task = ScrapyCrawlingTask(
        log_file_path=log_file_path, start_time=start_time)
    result = task(spider_names=spider_names, spider_kwargs=spider_kwargs)


flow.run(parameters=dict(
    spider_names=[
        'sankei_com_sitemap', 'asahi_com_sitemap', 'kyodo_co_jp_sitemap', 'jp_reuters_com_crawl', 'yomiuri_co_jp_sitemap', 'epochtimes_jp_sitemap',
    ],
    spider_kwargs={
        'debug': 'Yes',
        'lastmod_period_minutes': '60,15',
        'pages': '2,2',
        # 'continued':'Yes',
        # 'direct_crawl_urls':[],
        # 'crawl_point_non_update':'Yes',
    }
))
