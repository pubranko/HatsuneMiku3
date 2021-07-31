
# 単一プロセスでcrawlerprocessを使った例（クラスバージョン）
import os
import sys
path = os.getcwd()
sys.path.append(path)
import io
import re
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from prefect import Flow, task, Task
import logging
from datetime import datetime
from news_crawl.spiders.epochtimes_jp_sitemap import EpochtimesJpSitemapSpider
from news_crawl.spiders.sankei_com_sitemap import SankeiComSitemapSpider
from news_crawl.models.mongo_model import MongoModel
from news_crawl.models.crawler_logs_model import CrawlerLogsModel
from news_crawl.settings import TIMEZONE
from news_crawl.spiders.common.mail_send import mail_send

prefect_flow_name: str = 'RegularObservation'
log_file_path = os.path.join(
    'logs', 'regular_observation_spider.log')
logging.basicConfig(level=logging.DEBUG, filemode="w+", filename=log_file_path,
                    format='%(asctime)s [%(name)s] %(levelname)s: %(message)s', datefmt='%Y-%m-%d %H:%M:%S')


class RegularObservation(Task):
    '''定期観測'''
    mongo = MongoModel()
    crawl_start_time: datetime
    log_file: str

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # クロール開始時間
        self.crawl_start_time = datetime.now().astimezone(
            TIMEZONE)

    def crawler_run(self):

        process = CrawlerProcess(settings=get_project_settings())
        process.crawl(SankeiComSitemapSpider,
                      crawl_start_time=self.crawl_start_time,
                      debug='Yes',
                      lastmod_recent_time='10',
                      url_term_days='1',)
        process.crawl(EpochtimesJpSitemapSpider,
                      crawl_start_time=self.crawl_start_time,
                      debug='Yes',
                      lastmod_recent_time='10',)
        process.start()

    def crawl_log_save(self):
        # logファイルオープン
        with open(log_file_path) as f:
            self.log_file = f.read()
        # クロール結果のログをMongoDBへ保存
        crawler_logs = CrawlerLogsModel(self.mongo)
        crawler_logs.insert_one({
            'crawl_start_time': self.crawl_start_time.isoformat(),
            'record_type': 'regular_observation_spider',
            'logs': self.log_file,
        })

    def crawl_log_check(self):
        #pattern = re.compile(r'^\[[0-9]+,[0-9]+\]$')
        #CRITICAL > ERROR > WARNING > INFO > DEBUG
        pattern_critical = re.compile(
            r'^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2} \[.*\] CRITICAL')
        pattern_error = re.compile(
            r'^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2} \[.*\] ERROR')
        pattern_warning = re.compile(
            r'^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2} \[.*\] WARNING')
        # 2021-07-31 20:43:48 [twisted] CRITICAL: Unhandled error in Deferred:

        logs = io.StringIO(self.log_file)
        title: str = ''
        msg: str = '\n'.join([
            '【ログ】', self.log_file,
        ])

        for line in logs.readlines():
            if pattern_critical.search(line):
                title = '【spider:クリティカル発生】' + self.crawl_start_time.isoformat()
            elif pattern_error.search(line):
                title = '【spider:エラー発生】' + self.crawl_start_time.isoformat()
            elif pattern_warning.search(line):
                title = '【spider:ワーニング発生】' + self.crawl_start_time.isoformat()

        if not title == '':
            mail_send(title, msg,)

    def end(self):
        # 終了処理
        self.mongo.close()

    def run(self,):

        self.crawler_run()
        self.crawl_log_save()
        self.crawl_log_check()
        self.end()


with Flow(prefect_flow_name) as flow:
    task = RegularObservation()
    result = task()

flow.run()

'''
【定時観測処理の流れ】
完・クローラー実行(news_crawl)
完・ログのMongoDBへの保存
完・同時にエラーがあればメール通知
　　※ログのファイル名切り替え随時
・メモリリーク問題
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