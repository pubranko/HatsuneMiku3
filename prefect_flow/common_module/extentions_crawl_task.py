# 単一プロセスでcrawlerprocessを使った例（クラスバージョン）
import os
import sys
path = os.getcwd()
sys.path.append(path)
import re
from prefect import Task
from prefect.engine import signals
import logging
from datetime import datetime
from common.mail_send import mail_send
from prefect_flow.common_module.regular_crawler_run import regular_crawler_run
from common.resource_check import resource_check
from news_crawl.models.mongo_model import MongoModel
from news_crawl.models.crawler_logs_model import CrawlerLogsModel

#logging.basicConfig(level=logging.DEBUG, filemode="w+", filename=log_file_path,
#                    format='%(asctime)s [%(name)s] %(levelname)s: %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

class ExtensionsCrawlTask(Task):
    '''定期観測'''
    mongo = MongoModel()
    crawl_start_time: datetime
    log_file: str
    log_file_path = os.path.join(
        'logs', 'extensions_crawl_task.log')

    def __init__(self, crawl_start_time: datetime, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # クロール開始時間
        if crawl_start_time:
            self.crawl_start_time = crawl_start_time
        else:
            raise signals.FAIL(message="引数エラー:crawl_start_timeが指定されていません。")

    def crawl_log_check(self):
        '''クリティカル、エラー、ワーニングがあったらメールで通知'''
        # logファイルオープン
        with open(self.log_file_path) as f:
            self.log_file = f.read()

        #CRITICAL > ERROR > WARNING > INFO > DEBUG
        pattern_critical = re.compile(
            r'^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2} \[.*\] CRITICAL')
        pattern_error = re.compile(
            r'^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2} \[.*\] ERROR')
        pattern_warning = re.compile(
            r'^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2} \[.*\] WARNING')
        # 2021-07-31 20:43:48 [twisted] CRITICAL: Unhandled error in Deferred:

        title: str = ''
        if pattern_critical.search(self.log_file):
            title = '【spider:クリティカル発生】' + self.crawl_start_time.isoformat()
        elif pattern_error.search(self.log_file):
            title = '【spider:エラー発生】' + self.crawl_start_time.isoformat()
        elif pattern_warning.search(self.log_file):
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

    def end(self):
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
        #return self.crawl_start_time
