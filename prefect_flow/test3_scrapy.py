# 単一プロセスでcrawlerprocessを使った例
import os
import sys
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from prefect import Flow, task
import logging
from datetime import datetime

file_path = os.path.join(
    'logs', 'regular_observation_spider.log')

logging.basicConfig(level=logging.DEBUG, filemode="w+", filename=file_path,
                    format='%(asctime)s [%(name)s] %(levelname)s: %(message)s')

@task
def regular_observation():
    '''定期観測'''

    path = os.getcwd()
    sys.path.append(path)
    from news_crawl.spiders.epochtimes_jp_sitemap import EpochtimesJpSitemapSpider
    from news_crawl.spiders.sankei_com_sitemap import SankeiComSitemapSpider
    from news_crawl.models.mongo_model import MongoModel
    from news_crawl.models.crawler_logs_model import CrawlerLogsModel
    from news_crawl.settings import TIMEZONE

    # クロール開始時間
    crawl_start_time = datetime.now().astimezone(
        TIMEZONE)

    process = CrawlerProcess(settings=get_project_settings())
    process.crawl(SankeiComSitemapSpider,
                  crawl_start_time=crawl_start_time,
                  debug='Yes',
                  lastmod_recent_time='10',
                  url_term_days='1',)
    process.crawl(EpochtimesJpSitemapSpider,
                  crawl_start_time=crawl_start_time,
                  debug='Yes',
                  lastmod_recent_time='10',)
    process.start()

    # logファイルオープン、MongoDBオープン
    with open(file_path) as f:
        log_file = f.read()
    mongo = MongoModel()
    # クロール結果のログをMongoDBへ保存
    crawler_logs = CrawlerLogsModel(mongo)
    crawler_logs.insert_one({
        'crawl_start_time': crawl_start_time.isoformat(),
        'record_type': 'regular_observation_spider',
        'logs':log_file,
    })
    mongo.close()




with Flow("RegularObservation") as flow:
    regular_observation()

flow.run()


'''
完：ログをタスクの単位でまとめることで妥協する。
完：出力したログをmongoDBへ吸い上げることで保存する。
完：ファイル名はクローラーの実行であることがわかる名前にする。
不要：タスクが終わったらファイルを消す。（テスト時は残したいかも、、、デバックオプションで残せるようにしよう）

【課題】
・spiderのclosedで保存したstatsはspider単位。タスク単位のログとどう合わせる？
・ログのフィルターでseleniumのやつとか消せる？


【定時観測処理の流れ】
・クローラー実行(news_crawl)
・ログのMongoDBへの保存
・同時にエラーがあればメール通知
※ログのファイル名切り替え随時
・スクレイピング(名前未定)
※ログのファイル名切り替え随時
・solrへ流し込み(名前未定)
※ログのファイル名切り替え随時

【ログでエラーやdebugが発生した場合の通知方法】
・scrapyに直接もたせたエラーのメール通知機能をprefectへへ移植

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