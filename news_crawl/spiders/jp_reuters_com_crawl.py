from news_crawl.spiders.extensions_class.extensions_crawl import ExtensionsCrawlSpider
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import Rule
from scrapy.http import Response
from scrapy_selenium import SeleniumRequest
import urllib.parse
import scrapy
from selenium.webdriver.remote.webdriver import WebDriver
from selenium.webdriver.remote.webelement import WebElement
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from news_crawl.spiders.common.start_request_debug_file_generate import start_request_debug_file_generate


class JpReutersComCrawlSpider(ExtensionsCrawlSpider):
    name: str = 'jp_reuters_com_crawl'
    allowed_domains: list = ['jp.reuters.com']
    start_urls: list = [
        # 'https://jp.reuters.com/news/archive?view=page&page=1&pageSize=10'  # 最新ニュース
        # 'https://jp.reuters.com/news/archive?view=page&page=2&pageSize=10' #2ページ目
    ]
    _domain_name: str = 'jp_reuters_com'        # 各種処理で使用するドメイン名の一元管理
    _spider_version: float = 1.0

    rules = (
        Rule(LinkExtractor(
            allow=(r'/article/')), callback='parse_news'),
    )

    def start_requests(self):
        '''
        start_urlsを使わずに直接リクエストを送る。
        '''
        # 開始ページからURLを生成
        pages: dict = self.pages_setting(1, 5)
        start_page: int = pages['start_page']
        url = 'https://jp.reuters.com/news/archive?view=page&page=' + \
            str(start_page) + '&pageSize=10'

        self.start_urls.append(url)

        yield SeleniumRequest(
            url=url,
            callback=self.parse_start_response,
            #errback=self.errback_handle,
        )

    def parse_start_response(self, response: Response):
        ''' (拡張メソッド)
        取得したレスポンスよりDBへ書き込み
        '''
        # ループ条件
        # 1.現在のページ数は、5ページまで（仮）
        # 2.前回の1ページ目の記事リンク（10件）まで全て遡りきったら、前回以降に追加された記事は取得完了と考えられるため終了。
        pages: dict = self.pages_setting(1, 5)
        start_page: int = pages['start_page']
        end_page: int = pages['end_page']

        driver: WebDriver = response.request.meta['driver']
        urls_list: list = []

        url_header = str(response.url).split('?')[0]

        # 前回からの続きの指定がある場合、前回の１ページ目の１０件のURLを取得する。
        last_time_urls: list = []
        if 'continued' in self.kwargs_save:
            last_time_urls = [
                _['loc'] for _ in self._next_crawl_point[url_header]['urls']]

        page:int = start_page
        while page <= end_page:
            self.logger.info(
                '=== parse_start_response 現在解析中のURL = %s', driver.current_url)

            # クリック対象が読み込み完了していることを確認   例）href="?view=page&amp;page=2&amp;pageSize=10"
            page += 1  # 次のページ数
            next_page_selecter: str = '.control-nav-next[href$="view=page&page=' + \
                str(page) + '&pageSize=10"]'
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located(
                    (By.CSS_SELECTOR, next_page_selecter))
            )

            # 現在のページ内の記事のリンクをリストへ保存
            links: list = driver.find_elements_by_css_selector(
                '.story-content a[href]')
            self.logger.info(
                '=== parse_start_response リンク件数 = %s', len(links))
            # ページ内リンクは通常10件。それ以外の場合はワーニングメール通知（環境によって違うかも、、、）
            if not len(links) == 10:
                #self.layout_change_notice(response)
                self.logger.warning(
                    '=== parse_start_response 1ページ内で取得できた件数が想定の10件と異なる。確認要。 ( %s 件)', len(links))

            for link in links:
                link: WebElement
                url: str = urllib.parse.unquote(link.get_attribute("href"))
                urls_list.append({'loc': url, 'lastmod': ''})

                # 前回取得したurlが確認できたら確認済み（削除）にする。
                if url in last_time_urls:
                    last_time_urls.remove(url)

            # 前回からの続きの指定がある場合、前回の１ページ目のurlが全て確認できたら前回以降に追加された記事は全て取得完了と考えられるため終了する。
            if 'continued' in self.kwargs_save:
                if len(last_time_urls) == 0:
                    self.logger.info(
                        '=== parse_start_response 前回の続きまで再取得完了 (%s)', driver.current_url)
                    break

            # 次のページを読み込む
            elem: WebElement = driver.find_element_by_css_selector(
                '.control-nav-next')
            elem.click()

        # リストに溜めたurlをリクエストへ登録する。
        for _ in urls_list:
            #yield scrapy.Request(response.urljoin(_['loc']), callback=self.parse_news, errback=self.errback_handle)
            yield scrapy.Request(response.urljoin(_['loc']), callback=self.parse_news,)
        # 次回向けに1ページ目の10件をcrawler_controllerへ保存する情報
        self._next_crawl_point[url_header] = {
            'urls': urls_list[0:10],
            'crawl_start_time': self._crawl_start_time.isoformat()
        }

        start_request_debug_file_generate(
            self.name, response.url, urls_list, self.kwargs_save)
