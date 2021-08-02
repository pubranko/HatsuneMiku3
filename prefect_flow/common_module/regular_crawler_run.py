import os
import sys
path = os.getcwd()
sys.path.append(path)
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from news_crawl.spiders.epochtimes_jp_sitemap import EpochtimesJpSitemapSpider
from news_crawl.spiders.sankei_com_sitemap import SankeiComSitemapSpider
from news_crawl.spiders.asahi_com_xml_feed import AsahiComXmlFeedSpider
from news_crawl.spiders.jp_reuters_com_sitemap import JpReutersComSitemapSpider
from news_crawl.spiders.kyodo_co_jp_sitemap import KyodoCoJpSitemapSpider
from news_crawl.spiders.yomiuri_co_jp_sitemap import YomiuriCoJpSitemapSpider

def regular_crawler_run(crawl_start_time):

    process = CrawlerProcess(settings=get_project_settings())
    process.crawl(SankeiComSitemapSpider,
                    crawl_start_time=crawl_start_time,
                    debug='Yes',
                    lastmod_recent_time='1',
                    url_term_days='1',)
    process.crawl(EpochtimesJpSitemapSpider,
                    crawl_start_time=crawl_start_time,
                    debug='Yes',
                    lastmod_recent_time='1',)
    process.start()
