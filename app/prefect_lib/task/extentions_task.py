# 単一プロセスでcrawlerprocessを使った例（クラスバージョン）
import os
import sys
import re
from typing import Any
from logging import Logger
from datetime import datetime
import prefect
from prefect.core.task import Task
from prefect.engine import signals
from prefect.utilities.context import Context
from prefect.utilities import context as prefect_utilities_con
path = os.getcwd()
sys.path.append(path)
from common_lib.mail_send import mail_send
from common_lib.resource_check import resource_check
from common_lib.environ_check import environ_check
from models.mongo_model import MongoModel
from models.crawler_logs_model import CrawlerLogsModel
from common_lib.common_settings import TIMEZONE
from common_lib.common_settings import NOTICE_LEVEL


class ExtensionsTask(Task):
    '''
    引数: log_file_path:str = 出力ログのファイルパス , start_time:datetime = 起点となる時間 ,
    notice_level = メール通知するログレベル[CRITICAL|ERROR|WARNING]
    '''
    mongo = MongoModel()
    start_time: datetime
    log_record: str  # 読み込んだログファイルオブジェクト
    log_file_path: str  # ログファイルのパス
    #prefect_context: Context = prefect.context
    any: Any = prefect
    prefect_context: Context = any.context
    logger: Logger

    # def __init__(self, log_file_path: str, start_time: datetime, *args, **kwargs):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # ログファイルパス
        # if log_file_path:
        #     self.log_file_path = log_file_path
        #     self.logger.info(
        #         f'=== ExtensionsTask __init__ log_file_path : {log_file_path}')
        # else:
        #     raise signals.FAIL(message="引数エラー:log_file_pathが指定されていません。")

        # 開始時間
        self.start_time=datetime.now().astimezone(TIMEZONE)
        # if start_time:
        #     self.start_time = start_time
        #     self.logger.info(
        #         f'=== ExtensionsTask __init__ start_time : {start_time.isoformat()}')
        # else:
        #     raise signals.FAIL(message="引数エラー:start_timeが指定されていません。")

        # 環境変数チェック
        environ_check()

    def log_check(self):
        '''クリティカル、エラー、ワーニングがあったらメールで通知'''
        # logファイルオープン
        with open(self.log_file_path) as f:
            self.log_record = f.read()
        #self.log_file = open (self.log_file_path)
        #self.log_record = self.log_file.read()

        #CRITICAL > ERROR > WARNING > INFO > DEBUG
        # 2021-08-08 12:31:04 [scrapy.core.engine] INFO: Spider closed (finished)
        # クリティカルの場合、ログ形式とは限らない。raiseなどは別形式のため、後日検討要。
        pattern_traceback = re.compile(r'Traceback.*:')
        pattern_critical = re.compile(
            r'[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2} CRITICAL ')
        pattern_error = re.compile(
            r'[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2} ERROR ')
        # pattern_warning = re.compile(
        #     r'[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2} WARNING ')
        # 2021-08-08 12:31:04 INFO [prefect.FlowRunner] : Flow run SUCCESS: all reference tasks succeeded

        title: str = ''
        if pattern_traceback.search(self.log_record) and NOTICE_LEVEL in ['CRITICAL']:
            title = f'【{self.name}:クリティカル発生】{self.start_time.isoformat()}'
        elif pattern_critical.search(self.log_record) and NOTICE_LEVEL in ['CRITICAL']:
            title = f'【{self.start_time.isoformat()}:クリティカル発生】{self.start_time.isoformat()}'
        elif pattern_error.search(self.log_record) and NOTICE_LEVEL in ['CRITICAL', 'ERROR']:
            title = f'【{self.name}:エラー発生】{self.start_time.isoformat()}'
        # elif pattern_warning.search(self.log_record) and NOTICE_LEVEL in ['CRITICAL', 'ERROR', 'WARNING']:
        #     title = f'【{self.name}:ワーニング発生】{self.start_time.isoformat()}'

        if not title == '':
            msg: str = '\n'.join([
                '【ログ】', self.log_record,
            ])
            mail_send(title, msg,)

    def log_save(self):
        '''処理が終わったらログを保存'''
        crawler_logs = CrawlerLogsModel(self.mongo)
        crawler_logs.insert_one({
            'start_time': self.start_time,
            'flow_name': self.prefect_context['flow_name'],
            'record_type': self.name,
            'logs': self.log_record,
        })

    def closed(self):
        '''終了処理'''

        self.log_check()
        resource_check()
        self.log_save()
        self.mongo.close()
        os.remove(self.log_file_path)  # 終了後ログファイルを削除

    def run_init(self):
        from prefect_lib.common_module.logging_setting import LOG_FILE_PATH
        # ログファイル
        self.log_file_path = LOG_FILE_PATH
        self.logger.info(
            f'=== ExtensionsTask run_init log_file_path : {self.log_file_path}')
        # 開始時間
        self.start_time=datetime.now().astimezone(TIMEZONE)
        self.logger.info(
            f'=== ExtensionsTask run_init start_time : {self.start_time}')

    def run(self,):
        '''(ここはオーバーライドすることを前提とする。)
        ここがprefectで起動するメイン処理
        '''
        self.run_init()
        pass
        self.closed()

