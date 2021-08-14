# 単一プロセスでcrawlerprocessを使った例
import os
import sys
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from scrapy.utils import log
import prefect
from prefect import Flow, task
#from models.mongo_model import MongoModel
#from models.mongo_model import MongoModel
#from models.crawler_logs_model import CrawlerLogsModel
#import logging

# logging.basicConfig(level=logging.DEBUG, filemode="w+", filename="logs/prefect.log",
#                     format='%(asctime)s [%(name)s] %(levelname)s: %(message)s')


@task
def regular_observation():
    '''定期観測'''

    path = os.getcwd()
    sys.path.append(path)
    from news_crawl.spiders.epochtimes_jp_sitemap import EpochtimesJpSitemapSpider
    from news_crawl.spiders.sankei_com_sitemap import SankeiComSitemapSpider


    #logger = prefect.context.get("logger")
    process = CrawlerProcess(settings=get_project_settings())

    log.dictConfig({
        "version": 1,
        "disable_existing_loggers": True,
        "loggers": {
            "prefect (INFO)": {
                "level": "INFO",
            }
        }
    })


    process.crawl(SankeiComSitemapSpider,
                  debug='Yes',
                  lastmod_recent_time='1',
                  url_term_days='1',
                  log=log)
    process.crawl(EpochtimesJpSitemapSpider,
                  debug='Yes',
                  lastmod_recent_time='1',
                  log=log)
    process.start()


with Flow("RegularObservation ") as flow:
    regular_observation()

flow.run()
