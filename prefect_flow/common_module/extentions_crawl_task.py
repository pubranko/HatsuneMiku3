# 単一プロセスでcrawlerprocessを使った例（クラスバージョン）
import os
import sys
path = os.getcwd()
sys.path.append(path)
import re
import prefect
from prefect import Task
from prefect.engine import signals
from logging import Logger
from datetime import datetime
from common.mail_send import mail_send
from common.resource_check import resource_check
from common.environ_check import environ_check
from news_crawl.models.mongo_model import MongoModel
from news_crawl.models.crawler_logs_model import CrawlerLogsModel
from prefect.utilities.context import Context


class ExtensionsCrawlTask(Task):
    '''
    引数: log_file_path:str = 出力ログのファイルパス , crawl_start_time:datetime = 起点となる時間 ,
    notice_level = メール通知するログレベル[CRITICAL|ERROR|WARNING]
    '''
    mongo = MongoModel()
    crawl_start_time: datetime
    log_file: str  # 読み込んだログファイルオブジェクト
    log_file_path: str  # ログファイルのパス
    notice_level: str
    prefect_context:Context = prefect.context

    def __init__(self, log_file_path: str, crawl_start_time: datetime, notice_level: str, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # ログファイルパス
        if log_file_path:
            self.log_file_path = log_file_path
        else:
            raise signals.FAIL(message="引数エラー:log_file_pathが指定されていません。")

        # クロール開始時間
        if crawl_start_time:
            self.crawl_start_time = crawl_start_time
        else:
            raise signals.FAIL(message="引数エラー:crawl_start_timeが指定されていません。")

        if notice_level in ['CRITICAL', 'ERROR', 'WARNING']:
            self.notice_level = notice_level
        else:
            raise signals.FAIL(
                message="引数エラー:notice_levelには[CRITICAL|ERROR|WARNING]のどれかを指定してください。")

        #環境変数チェック
        environ_check()

    def crawl_log_check(self):
        '''クリティカル、エラー、ワーニングがあったらメールで通知'''
        # logファイルオープン
        with open(self.log_file_path) as f:
            self.log_file = f.read()

        #CRITICAL > ERROR > WARNING > INFO > DEBUG
        #2021-08-08 12:31:04 [scrapy.core.engine] INFO: Spider closed (finished)
        pattern_critical = re.compile(
            r'^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}\+[0-9]{4} CRITICAL ')
        pattern_error = re.compile(
            r'^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}\+[0-9]{4} ERROR ')
        pattern_warning = re.compile(
            r'^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}\+[0-9]{4} WARNING ')
        # 2021-08-08 12:31:04 INFO [prefect.FlowRunner] : Flow run SUCCESS: all reference tasks succeeded

        title: str = ''
        if pattern_critical.search(self.log_file):
            title = '【spider:クリティカル発生】' + self.crawl_start_time.isoformat()
        elif pattern_error.search(self.log_file) and self.notice_level in ['CRITICAL', 'ERROR']:
            title = '【spider:エラー発生】' + self.crawl_start_time.isoformat()
        elif pattern_warning.search(self.log_file) and self.notice_level in ['CRITICAL', 'ERROR', 'WARNING']:
            title = '【spider:ワーニング発生】' + self.crawl_start_time.isoformat()

        if not title == '':
            msg: str = '\n'.join([
                '【ログ】', self.log_file,
            ])
            mail_send(title, msg,)

    def crawl_log_save(self):
        '''クロールが終わったらログを保存'''
        # クロール結果のログをMongoDBへ保存
        crawler_logs = CrawlerLogsModel(self.mongo)
        crawler_logs.insert_one({
            'crawl_start_time': self.crawl_start_time.isoformat(),
            'record_type': 'regular_observation_task',
            'logs': self.log_file,
        })

    def closed(self):
        '''終了処理'''

        self.crawl_log_check()
        resource_check()
        self.crawl_log_save()

        self.mongo.close()

    def run(self,):
        '''(ここはオーバーライドすることを前提とする。)
        ここがprefectで起動するメイン処理
        '''
        pass
