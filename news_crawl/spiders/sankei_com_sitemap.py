from news_crawl.spiders.extensions_class.extensions_sitemap import ExtensionsSitemapSpider
from scrapy.spiders import Rule
from scrapy.linkextractors import LinkExtractor

from scrapy.spiders.sitemap import iterloc
from scrapy.http import Request
from scrapy.http import Response
from scrapy.utils.sitemap import Sitemap, sitemap_urls_from_robots
from scrapy.utils.gz import gunzip, gzip_magic_number
from news_crawl.items import NewsCrawlItem
import pickle
import scrapy
import re
import scrapy
from datetime import datetime, timedelta
from dateutil import parser
from selenium.webdriver.remote.webdriver import WebDriver
from selenium.webdriver.remote.webelement import WebElement
from scrapy_selenium import SeleniumRequest


class SankeiComSitemapSpider(ExtensionsSitemapSpider):
    name: str = 'sankei_com_sitemap'
    allowed_domains: list = ['sankei.com']
    sitemap_urls: list = [
        'https://www.sankei.com/robots.txt',
        # 'https://feed.etf.sankei.com/global/sitemap',
    ]
    _domain_name: str = 'sankei_com'        # 各種処理で使用するドメイン名の一元管理
    _spider_version: float = 1.0

    custom_settings: dict = {
        'DEPTH_LIMIT': 2,
        'DEPTH_STATS_VERBOSE': True,
        # 'LOG_FILE' : 'logs/test_log('+ name +').txt',
    }
    # seleniumモードON。callbackをselenium用parseに変更。
    selenium_mode: bool = True
    rules = (Rule(LinkExtractor(allow=(r'.+')), callback='selenium_parse'),)
