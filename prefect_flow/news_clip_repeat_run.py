from news_crawl.models import crawler_scheduler_model
import os
import sys
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from prefect import Flow, task

from importlib import import_module

@task
def crawl_run():
    path = os.getcwd()
    sys.path.append(path)
    from news_crawl.models.mongo_model import MongoModel
    from news_crawl.models.crawler_scheduler_model import CrawlerSchedulerModel

    mongo = MongoModel()
    #
    crawler_scheduler = CrawlerSchedulerModel(mongo)
    crawler_scheduler_records = crawler_scheduler.find({})


    #クローラーごとにprocess部分を外出し？DBから取得する？
    #あと動的モジュールインポートで一括で起動させたい。

    from news_crawl.spiders.epochtimes_jp_sitemap import EpochtimesJpSitemapSpider

    process = CrawlerProcess(settings=get_project_settings())
    process.crawl(
        EpochtimesJpSitemapSpider,
        debug='Yes',
        lastmod_recent_time='180',
        # category_urls='[108]',
    )
    process.start()


with Flow("NewsClipRepeatRun") as flow:
    crawl_run()

flow.run()
