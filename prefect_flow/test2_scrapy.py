import os
import sys
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from prefect import Flow, task

from importlib import import_module

from multiprocessing import Process


@task
def crawl():

    def test():
        # scrapy crawl sankei_com_sitemap -a lastmod_recent_time=10 -a url_term_days=1 -a debug=Yes
        process = CrawlerProcess(settings=get_project_settings())
        process.crawl(
            SankeiComSitemapSpider,
            debug='Yes',
            lastmod_recent_time='5',
            url_term_days='1',
            # category_urls='[108]',
        )
        process.start()

    path = os.getcwd()
    sys.path.append(path)
    from news_crawl.spiders.epochtimes_jp_sitemap import EpochtimesJpSitemapSpider
    from news_crawl.spiders.sankei_com_sitemap import SankeiComSitemapSpider

    p = Process(
        target=test,
        #args=("Message: call execute_anothoer_process()!",)
    )
    p.start()


with Flow("Test2Scrapy") as flow:
    crawl()

flow.run()
