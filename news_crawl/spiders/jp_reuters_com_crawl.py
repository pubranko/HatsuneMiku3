from typing import Type
from news_crawl.spiders.extensions_crawl import ExtensionsCrawlSpider
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import Rule
from scrapy.http import Response
from scrapy_selenium import SeleniumRequest
from news_crawl.items import NewsCrawlItem
from datetime import datetime
import pickle

from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import NoSuchElementException
from bs4 import BeautifulSoup as bs4
from bs4.element import ResultSet


class JpReutersComCrawlSpider(ExtensionsCrawlSpider):
    name:str = 'jp_reuters_com_crawl'
    allowed_domains:list = ['jp.reuters.com']
    start_urls:list = [
         #'http://jp.reuters.com/',
         'https://jp.reuters.com/theWire',
         #'https://jp.reuters.com/news/topNews',
         ]
    _domain_name: str = 'jp_reuters_com_crawl'        # 各種処理で使用するドメイン名の一元管理
    spider_version: float = 1.0

    # sitemap_urlsに複数のサイトマップを指定した場合、その数だけsitemap_filterが可動する。その際、どのサイトマップか判別できるように処理中のサイトマップと連動するカウント。
    _crawl_urls_count: int = 0
    # crawler_controllerコレクションへ書き込むレコードのdomain以降のレイアウト雛形。※最上位のKeyのdomainはサイトの特性にかかわらず固定とするため。
    _cwawl_next_info: dict = {name: {}, }

    rules = (
        #Rule(LinkExtractor(
        #    allow=(r'/theWire')), callback='parse_top_news'),
        Rule(LinkExtractor(
            allow=(r'/article/')), callback='parse_news'),
    )

    def start_requests(self):
        '''start_urlsを使わずに直接リクエストを送る。
        あとで
        '''
        yield SeleniumRequest(
            url='https://jp.reuters.com/theWire',
            callback=self.parse_start_response,
            #callback=self.parse,
            wait_time=10,
            wait_until=EC.element_to_be_clickable((By.CLASS_NAME, 'load-more-content')),
            #screenshot=True,
            )

        ### class="load-more-link
        # <div class="load-more-link load-more-wire wire-container">
        # <span class="load-more-content active">
        #     <div class="more-load">LOAD MORE</div>
        #     <div class="more-arrow">∨</div>
        # </span>
        # <div class="loader-container loader-wire">
        #     <div class="loader"><div class="loader-dot"></div><div class="loader-dot"></div><div class="loader-dot"></div></div>
        # </div>
        # </div>
        #yield SeleniumRequest('https://jp.reuters.com/news/topNews',self.parse_top_news,)
        #yield SeleniumRequest('https://jp.reuters.com/news/topNews',self.parse_top_news,)
        '''
        (class) SeleniumRequest(
            wait_time: Unknown = None,
            wait_until: Unknown = None,
            screenshot: Unknown = False,
            script: Unknown = None,
            *args: Unknown, **kwargs: Unknown)
        これらを使用すると、seleniumはスパイダーにレスポンスを返す前に 明示的なウエイトを実行します。
        yield SeleniumRequest(
            url=url,
            callback=self.parse_result,
            wait_time=10,
            wait_until=EC.element_to_be_clickable((By.ID, 'someid'))
        )
        これを使用すると、seleniumはページのスクリーンショットを撮り、キャプチャしたPNG画像のバイナリデータが response meta に追加されます。
        yield SeleniumRequest(
           url=url,
            callback=self.parse_result,
            screenshot=True
        )
        これを使用すると、seleniumはカスタムJavaScriptコードを実行します。
        yield SeleniumRequest(
           url=url,
            callback=self.parse_result,
            script='window.scrollTo(0, document.body.scrollHeight);',
        )
        '''


    # def parse_start_url(self, response: Response):
    #     '''
    #     start_urls自体のレスポンスの処理
    #     '''
    #     self.common_prosses(self.start_urls[self._crawl_urls_count], response)
    #     self._crawl_urls_count += 1  # 次のurl用にカウントアップ



    def parse_start_response(self, response: Response):
        ''' (拡張メソッド)
        取得したレスポンスよりDBへ書き込み
        '''
        soup = bs4(response.body, 'lxml')

        links: ResultSet = soup.find_all('li')
        print('=== ',links.count)

        self.common_prosses(self.start_urls[self._crawl_urls_count], response)

        _info = self.name + ':' + str(self.spider_version) + ' / ' \
            + 'extensions_crawl:' + str(self._extensions_crawl_version)

        #with open('image.png', 'wb') as image_file:
        #    image_file.write(response.meta['screenshot'])

        yield NewsCrawlItem(
            url=response.url,
            response_time=datetime.now().astimezone(self.settings['TIMEZONE']),
            response_headers=pickle.dumps(response.headers),
            response_body=pickle.dumps(response.body),
            spider_version_info=_info
        )
