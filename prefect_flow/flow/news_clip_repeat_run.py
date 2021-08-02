# 単一プロセスでcrawlerprocessを使った例
import os
import sys
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from prefect import Flow, task
from news_crawl.models.mongo_model import MongoModel
from news_crawl.models.crawler_logs_model import CrawlerLogsModel
import logging

logging.basicConfig(level=logging.DEBUG, filemode="w+", filename="logs/prefect.log",
                    format='%(asctime)s [%(name)s] %(levelname)s: %(message)s')

@task
def regular_observation():
    '''定期観測'''

    path = os.getcwd()
    sys.path.append(path)
    from news_crawl.spiders.epochtimes_jp_sitemap import EpochtimesJpSitemapSpider
    from news_crawl.spiders.sankei_com_sitemap import SankeiComSitemapSpider

    process = CrawlerProcess(settings=get_project_settings())
    process.crawl(SankeiComSitemapSpider,
                  debug='Yes',
                  lastmod_recent_time='10',
                  url_term_days='1',)
    process.crawl(EpochtimesJpSitemapSpider,
                  debug='Yes',
                  lastmod_recent_time='10',)
    process.start()


with Flow("RegularObservation ") as flow:
    regular_observation()

flow.run()
