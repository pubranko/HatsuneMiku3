#単一プロセスで、runnerを使ったケース
import os
import sys
import time
from scrapy.crawler import CrawlerRunner
from scrapy.utils.log import configure_logging
from scrapy.utils.project import get_project_settings
from twisted.internet import reactor
from prefect import Flow, task
from importlib import import_module


@task
def crawl():
    print('=== メインプロセス(%s)' % os.getpid())
    path = os.getcwd()
    sys.path.append(path)
    from news_crawl.spiders.epochtimes_jp_sitemap import EpochtimesJpSitemapSpider
    from news_crawl.spiders.sankei_com_sitemap import SankeiComSitemapSpider

    configure_logging({'LOG_FORMAT': '%(levelname)s: %(message)s'})
    runner = CrawlerRunner(settings=get_project_settings())
    runner.crawl(
        SankeiComSitemapSpider,
        debug='Yes',
        lastmod_recent_time='20',
        url_term_days='1',
    )
    runner.crawl(
        EpochtimesJpSitemapSpider,
        debug='Yes',
        lastmod_recent_time='40',
        )
    d = runner.join()
    d.addBoth(lambda _: reactor.stop())
    reactor.run()



with Flow("Test4Scrapy") as flow:
    crawl()

flow.run()
