# 単一プロセスでcrawlerprocessを使った例
import os
import sys
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from prefect import Flow, task


@task
def crawl():

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


with Flow("Test3Scrapy") as flow:
    crawl()

flow.run()
