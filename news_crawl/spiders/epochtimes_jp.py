from news_crawl.spiders.extensions_sitemap import ExtensionsSitemapSpider
from news_crawl.items import NewsCrawlItem
from datetime import datetime
import pickle
from scrapy.http.response.html import HtmlResponse
from scrapy_selenium import SeleniumRequest
from selenium.webdriver.remote.webdriver import WebDriver
from selenium.webdriver.remote.webelement import WebElement
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC

from scrapy.http import Request
from scrapy.utils.sitemap import Sitemap, sitemap_urls_from_robots
from scrapy.spiders.sitemap  import iterloc

class EpochtimesJpSpider(ExtensionsSitemapSpider):
    name = 'epochtimes_jp'
    allowed_domains = ['epochtimes.jp']
    #start_urls = ['http://epochtimes.jp/']
    sitemap_urls: list = [
        'https://www.epochtimes.jp/sitemap/sitemap-latest.xml', #最新
    ]
    _domain_name: str = 'epochtimes_jp'        # 各種処理で使用するドメイン名の一元管理
    spider_version: float = 1.0
    custom_settings: dict = {
        'DEPTH_LIMIT': 2,
        'DEPTH_STATS_VERBOSE': True,
        #リダイレクト
        'REDIRECT_ENABLED' : True,
        'REDIRECT_MAX_TIMES' : 20,
        'COOKIES_DEBUG':True,
    }

    # sitemap_urlsに複数のサイトマップを指定した場合、その数だけsitemap_filterが可動する。その際、どのサイトマップか判別できるように処理中のサイトマップと連動するカウント。
    _sitemap_urls_count: int = 0
    # crawler_controllerコレクションへ書き込むレコードのdomain以降のレイアウト雛形。※最上位のKeyのdomainはサイトの特性にかかわらず固定とするため。
    _sitemap_next_crawl_info: dict = {name: {}, }

    def start_requests(self):
        for url in self.sitemap_urls:
            #yield Request(url, self._parse_sitemap)
            yield SeleniumRequest(url=url, callback=self._parse_sitemap)
            # yield SeleniumRequest(
            #     url=url,
            #     callback=self.parse_start_response,
            # )

    def parse(self, response:HtmlResponse):
        '''
        取得したレスポンスよりDBへ書き込み
        '''

        driver: WebDriver = response.request.meta['driver']
        print('=== parse :',type(driver))
        # Request.meta の redirect_urls キ
        print('=== parse :',type(response.meta))
        print('=== parse :',response.meta)

        _info = self.name + ':' + str(self.spider_version) + ' / ' \
            + 'extensions_sitemap:' + str(self._extensions_sitemap_version)

        yield NewsCrawlItem(
            url=response.url,
            response_time=datetime.now().astimezone(self.settings['TIMEZONE']),
            response_headers=pickle.dumps(response.headers),
            response_body=pickle.dumps(response.body),
            spider_version_info=_info
        )

    def _parse_sitemap(self, response):
        if response.url.endswith('/robots.txt'):
            for url in sitemap_urls_from_robots(response.text, base_url=response.url):
                yield Request(url, callback=self._parse_sitemap)
        else:
            body = self._get_sitemap_body(response)
            if body is None:
                self.logger.warning("Ignoring invalid sitemap: %(response)s",
                               {'response': response}, extra={'spider': self})
                return

            s = Sitemap(body)
            it = self.sitemap_filter(s)

            if s.type == 'sitemapindex':
                for loc in iterloc(it, self.sitemap_alternate_links):
                    if any(x.search(loc) for x in self._follow):
                        yield Request(loc, callback=self._parse_sitemap)
            elif s.type == 'urlset':
                for loc in iterloc(it, self.sitemap_alternate_links):
                    for r, c in self._cbs:
                        if r.search(loc):
                            #yield Request(loc, callback=c)
                            yield SeleniumRequest(url=loc, callback=c)
                            break
