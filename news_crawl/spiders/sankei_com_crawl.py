from news_crawl.spiders.extensions_crawl import ExtensionsCrawlSpider
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import Rule
from scrapy.http import Response
from scrapy_selenium import SeleniumRequest
from dateutil import parser
import urllib.parse
import scrapy
import sys
from selenium.webdriver.remote.webdriver import WebDriver
from selenium.webdriver.remote.webelement import WebElement
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from news_crawl.spiders.function.start_request_debug_file_generate import start_request_debug_file_generate


class SankeiComCrawlSpider(ExtensionsCrawlSpider):
    name = 'sankei_com_crawl'
    allowed_domains = ['sankei.com']
    start_urls = [
        # 'http://sankei.com/',
        # 'https://www.sankei.com/flash/',  # 速報
        # 'https://www.sankei.com/affairs/',  # 社会
        # 'https://www.sankei.com/politics/',  # 政治
        # 'https://www.sankei.com/world/',  # 国際
        # 'https://www.sankei.com/economy/',  # 経済
        # 'https://www.sankei.com/sports/',  # スポーツ
        # 'https://www.sankei.com/entertainments/',  # エンタメ
        # 'https://www.sankei.com/life/',  # ライフ
        # 'https://www.sankei.com/column/editorial/',  # 主張
        # 'https://www.sankei.com/column/seiron/',  # 正論
    ]

    # 続きボタン、ページありのサンプル
    # https://www.sankei.com/article/20210612-63TRGI366BPD5F2QJOR5EVC7RI/

    _domain_name: str = 'sankei_com'        # 各種処理で使用するドメイン名の一元管理
    _spider_version: float = 1.0

    # start_urlsまたはstart_requestの数。起点となるurlを判別するために使う。
    _crawl_urls_count: int = 0
    # crawler_controllerコレクションへ書き込むレコードのdomain以降のレイアウト雛形。※最上位のKeyのdomainはサイトの特性にかかわらず固定とするため。
    _next_crawl_info: dict = {}

    def __init__(self, *args, **kwargs):
        ''' (拡張メソッド)
        親クラスの__init__処理後に追加で初期処理を行う。
        '''
        super().__init__(*args, **kwargs)

        # 単項目チェック（追加）
        if not 'category_urls' in kwargs:
            sys.exit('引数エラー：当スパイダー(' + self.name +
                     ')の場合、category_urlsは必須です。')

    def start_requests(self):
        '''
        start_urlsを使わずに直接リクエストを送る。
        '''

        # [flash, affairs, politics, world, economy, sports, entertainments, life, column/editorial, column/seiron,]
        category_str: str = self.kwargs_save['category_urls']
        category_urls: list = category_str.lstrip('[').rstrip(']').split(',')
        urls: list = ['https://www.sankei.com/%s' %
                      str(i) + '/' for i in category_urls]

        for url in urls:
            self.start_urls.append(url)
            yield SeleniumRequest(
                url=url,
                callback=self.parse_start_response,
                errback=self.errback_handle,
            )

    def parse_start_response(self, response: Response):
        ''' (拡張メソッド)
        取得したレスポンスよりDBへ書き込み
        '''
        # ループ条件
        # 1.現在のページ数は、3ページまで（仮）
        # 2.前回の1ページ目の記事リンク（10件）まで全て遡りきったら、前回以降に追加された記事は取得完了と考えられるため終了。
        # ※続きボタン押下してajaxで追加される20件を次のページのデータとして取り扱う。
        pages: dict = self.pages_setting(1, 2)
        start_page: int = pages['start_page']
        end_page: int = pages['end_page']

        driver: WebDriver = response.request.meta['driver']
        links: list = []  # 最終的にリクエストを行いたいURLのリスト
        urls_list: list = []

        # 前回からの続きの指定がある場合、前回の１０件のURLを取得する。
        last_time_urls: list = []
        if 'continued' in self.kwargs_save:
            last_time_urls = [
                _['loc'] for _ in self._next_crawl_info[self.name][response.url]['urls']]

        page = start_page
        self.logger.info(
            '=== parse_start_response 現在解析中のpage=%s と URL = %s', page, driver.current_url)
        while page <= end_page:  # 条件はあとで考える

            page += 1  # 次のページ数

            elements = driver.find_elements_by_css_selector(
                '#fusion-app > div > div > section > .story-card-feed .storycard .story-card-flex')
            print('=== elementsの件数 = ', len(elements))
            for element in elements:
                element: WebElement
                url:str = element.find_element_by_css_selector('h4 a[href]').get_attribute('href')
                #url_list.append(url.get_attribute('href'))
                title = element.find_element_by_css_selector('h4 a').text
                _ = element.find_element_by_css_selector(
                    '.under-headline time')
                lastmod = _.get_attribute('datetime')
                lastmod_parse = parser.parse(lastmod).astimezone(
                    self.settings['TIMEZONE'])
                # ページ内のURLと更新日時をリストに保存する。
                urls_list.append({'loc': url, 'lastmod': lastmod_parse.isoformat})
                print('=== ', lastmod_parse, '  ', url, '  ', title)
                # 前回取得したurlが確認できたら確認済み（削除）にする。
                if url in last_time_urls:
                    last_time_urls.remove(url)

            self.logger.info(
                '=== parse_start_response リンク件数 = %s', len(urls_list))

            # 前回からの続きの指定がある場合、前回の１ページ目のurlが全て確認できたら前回以降に追加された記事は全て取得完了と考えられるため終了する。
            if 'continued' in self.kwargs_save:
                if len(last_time_urls) == 0:
                    self.logger.info(
                        '=== parse_start_response 前回の続きまで再取得完了 (%s)', driver.current_url)
                    break

            # 次のページを読み込む
            elem: WebElement = driver.find_element_by_css_selector(
                '.feedPagination')
            elem.click()

        # # リストに溜めたurlをリクエストへ登録する。
        # for _ in urls_list:
        #     yield scrapy.Request(response.urljoin(_['loc']), callback=self.parse_news, errback=self.errback_handle)
        # # 次回向けに1ページ目の10件をcrawler_controllerへ保存する情報
        # self._next_crawl_info[self.name][response.url] = {
        #     'urls': urls_list[0:10],
        #     'crawl_start_time': self._crawl_start_time.isoformat()
        # }

        # start_request_debug_file_generate(
        #     self.name, response.url, urls_list, self.kwargs_save)
