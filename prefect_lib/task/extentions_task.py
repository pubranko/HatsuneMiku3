# 単一プロセスでcrawlerprocessを使った例（クラスバージョン）
import os
import sys
path = os.getcwd()
sys.path.append(path)
import re
from logging import Logger
from datetime import datetime
import prefect
from prefect import Task, Parameter
from prefect.engine import signals
from prefect.utilities.context import Context
from common_lib.mail_send import mail_send
from common_lib.resource_check import resource_check
from common_lib.environ_check import environ_check
from models.mongo_model import MongoModel
from models.crawler_logs_model import CrawlerLogsModel
from prefect_lib.settings import NOTICE_LEVEL


class ExtensionsTask(Task):
    '''
    引数: log_file_path:str = 出力ログのファイルパス , crawl_start_time:datetime = 起点となる時間 ,
    notice_level = メール通知するログレベル[CRITICAL|ERROR|WARNING]
    '''
    mongo = MongoModel()
    starting_time: datetime
    log_file: str  # 読み込んだログファイルオブジェクト
    log_file_path: str  # ログファイルのパス
    prefect_context: Context = prefect.context

    def __init__(self, log_file_path: str, starting_time: datetime, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # ログファイルパス
        if log_file_path:
            self.log_file_path = log_file_path
        else:
            raise signals.FAIL(message="引数エラー:log_file_pathが指定されていません。")

        # 開始時間
        if starting_time:
            self.starting_time = starting_time
        else:
            raise signals.FAIL(message="引数エラー:crawl_start_timeが指定されていません。")

        # 環境変数チェック
        environ_check()

    def log_check(self):
        '''クリティカル、エラー、ワーニングがあったらメールで通知'''
        # logファイルオープン
        with open(self.log_file_path) as f:
            self.log_file = f.read()

        #CRITICAL > ERROR > WARNING > INFO > DEBUG
        # 2021-08-08 12:31:04 [scrapy.core.engine] INFO: Spider closed (finished)
        # クリティカルの場合、ログ形式とは限らない。raiseなどは別形式のため、後日検討要。
        pattern_critical = re.compile(
            r'^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2} CRITICAL ')
        pattern_error = re.compile(
            r'^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2} ERROR ')
        pattern_warning = re.compile(
            r'^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2} WARNING ')
        # 2021-08-08 12:31:04 INFO [prefect.FlowRunner] : Flow run SUCCESS: all reference tasks succeeded

        title: str = ''
        if pattern_critical.search(self.log_file):
            title = '【' + self.name + ':クリティカル発生】' + self.starting_time.isoformat()
        elif pattern_error.search(self.log_file) and NOTICE_LEVEL in ['CRITICAL', 'ERROR']:
            title = '【' + self.name + ':エラー発生】' + self.starting_time.isoformat()
        elif pattern_warning.search(self.log_file) and NOTICE_LEVEL in ['CRITICAL', 'ERROR', 'WARNING']:
            title = '【' + self.name + ':ワーニング発生】' + self.starting_time.isoformat()

        if not title == '':
            msg: str = '\n'.join([
                '【ログ】', self.log_file,
            ])
            mail_send(title, msg,)

    def log_save(self):
        '''処理が終わったらログを保存'''
        crawler_logs = CrawlerLogsModel(self.mongo)
        crawler_logs.insert_one({
            'crawl_start_time': self.starting_time.isoformat(),
            'record_type': 'regular_observation_task',
            'logs': self.log_file,
        })

    def closed(self):
        '''終了処理'''

        self.log_check()
        resource_check()
        self.log_save()
        self.mongo.close()

    def run(self,):
        '''(ここはオーバーライドすることを前提とする。)
        ここがprefectで起動するメイン処理
        '''
        pass