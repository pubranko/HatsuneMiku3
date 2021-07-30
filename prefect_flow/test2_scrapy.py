#マルチプロセスで動かしてみたい、、、
import os
import sys
import time
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from prefect import Flow, task
from importlib import import_module
from multiprocessing import Process
import logging
from scrapy.crawler import CrawlerRunner
from scrapy.utils.log import configure_logging
from twisted.internet import reactor
from twisted.internet.defer import Deferred
#from twisted.internet.epollreactor import EPollReactor

@task
def crawl():

    def test1(*args,**kwargs):
        print('=== test1プロセス(%s)' % os.getpid())
        configure_logging({'LOG_FORMAT': '%(levelname)s: %(message)s'})
        runner = CrawlerRunner(settings=get_project_settings())
        d:Deferred = runner.crawl(SankeiComSitemapSpider,debug='Yes',lastmod_recent_time='1',url_term_days='1',)
        d:Deferred = runner.crawl(EpochtimesJpSitemapSpider,debug='Yes',lastmod_recent_time='1',)
        #print('=== runner ',type(d))
        #print('=== runner ',d)
        d.addBoth(lambda _: reactor.stop())
        reactor.callFromThread(notThreadSafe,3)
        reactor.run()
        #print('=== reactor.run() ',type(reactor))


    print('=== メインプロセス(%s)' % os.getpid())
    path = os.getcwd()
    sys.path.append(path)
    from news_crawl.spiders.epochtimes_jp_sitemap import EpochtimesJpSitemapSpider
    from news_crawl.spiders.sankei_com_sitemap import SankeiComSitemapSpider

    jobs = []
    p = Process(
        #target=test1,
        target=test1(
            SankeiComSitemapSpider,
            debug='Yes',
            lastmod_recent_time='1',
            url_term_days='1',
        )
    )
    jobs.append(p)
    p.start()

    #time.sleep(5)
    for proc in jobs:
        print(type(proc))
        print('!!!',proc)



with Flow("Test2Scrapy") as flow:
    crawl()

flow.run()
