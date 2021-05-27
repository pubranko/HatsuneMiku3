from typing import Pattern
from bs4.element import ResultSet
from news_crawl.spiders.extensions_crawl import ExtensionsCrawlSpider
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import Rule
from scrapy.http.response.html import HtmlResponse
from scrapy_selenium import SeleniumRequest
import urllib.parse
import re
import scrapy
from selenium.webdriver.remote.webdriver import WebDriver
from selenium.webdriver.remote.webelement import WebElement
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
import concurrent.futures
from bs4 import BeautifulSoup as bs4


class EpochtimesJpCrawlSpider(ExtensionsCrawlSpider):
    name: str = 'epochtimes_jp_crawl'
    allowed_domains: list = ['epochtimes.jp']
    start_urls: list = [
        # 'https://www.epochtimes.jp/category/100/1.html'
        # 'https://www.epochtimes.jp/category/108/1.html'
        # 'https://www.epochtimes.jp/category/170/1.html'
        # 'https://www.epochtimes.jp/category/169/1.html'
        # 'https://www.epochtimes.jp/category/101/1.html'
    ]
    _domain_name: str = 'epochtimes_jp'        # 各種処理で使用するドメイン名の一元管理
    _spider_version: float = 1.0

    # start_urlsまたはstart_requestの数。起点となるurlを判別するために使う。
    _crawl_urls_count: int = 0
    # crawler_controllerコレクションへ書き込むレコードのdomain以降のレイアウト雛形。※最上位のKeyのdomainはサイトの特性にかかわらず固定とするため。
    _crawl_next_info: dict = {name: {}, }

    def start_requests(self):
        '''
        start_urlsを使わずに直接リクエストを送る。
        '''
        # 開始ページからURLを生成
        pages: dict = self.pages_setting(1, 5)
        start_page: int = pages['start_page']

        #urls = ['https://www.epochtimes.jp/category/%s' % str(i) + '/' + str(start_page) + '.html' for i in [100, 108, 170, 169, 101]]
        urls = ['https://www.epochtimes.jp/category/%s' %
                str(i) + '/' + str(start_page) + '.html' for i in [100]]  # テスト用

        for url in urls:
            self.start_urls.append(url)

            yield scrapy.Request(url, self.parse_start_response)
            # yield SeleniumRequest(
            #     url=url,
            #     callback=self.parse_start_response,
            # )

    def parse_start_response(self, response: HtmlResponse):
        ''' (拡張メソッド)
        取得したレスポンスよりDBへ書き込み
        '''
        # ループ条件
        # 1.現在のページ数は、5ページまで（仮）
        # 2.前回の1ページ目の記事リンク（10件）まで全て遡りきったら、前回以降に追加された記事は取得完了と考えられるため終了。

        pages: dict = self.pages_setting(1, 3)
        start_page: int = pages['start_page']
        end_page: int = pages['end_page']

        urls_list: list = []
        soup = bs4(response.text, 'html.parser')

        self.logger.info(
            '=== parse_start_response 現在処理中のURL = %s', response.url)
        # 現在の処理中のurlを３分割。（ヘッダー、ページ、フッター）
        pattern: Pattern = re.compile(r'https://www.epochtimes.jp/category/[0-9]{3}/')
        _ = pattern.sub('', response.url)
        pattern: Pattern = re.compile(r'\.html')
        now_page: int = int(pattern.sub('', _))

        url_header:str = re.findall(r'https://www.epochtimes.jp/category/[0-9]{3}/',response.url)[0]
        url_footer:str = re.findall(r'\.html',response.url)[0]

        print('=== 現在のurlを分解 = ', url_header, now_page, url_footer)

        # 前回からの続きの指定がある場合、前回の１ページ目の30件のURLを取得する。
        last_time_urls: list = []
        if 'continued' in self.kwargs_save:
            last_time_urls: list = self._crawler_controller_recode[
                self.name][self.start_urls[self._crawl_urls_count]]['urls']

        # 現在のページ内の記事のリンクをリストへ保存
        # １件目のアンカーと２件目以降のアンカーを取得。（１ページ目だけ１件目がテーブル内に無い）
        # 絶対パスに変換する。その際、リダイレクト先のURLへ直接リンクするよう、ドメイン/pを付与する。
        anchors: ResultSet = soup.select(
            '.category-left > a,table.table.table-hover tr > td[style] > a')
        for anchor in anchors:
            url: str = 'https://www.epochtimes.jp/p' + anchor.get('href')
            urls_list.append(url)
            # 前回取得したurlが確認できたら確認済み（削除）にする。
            if url in last_time_urls:
                last_time_urls.remove(url)

        # 前回からの続きの指定がある場合、前回の１ページ目のurlが全て確認できたら前回以降に追加された記事は全て取得完了と考えられるため終了する。
        end_flg = False
        if 'continued' in self.kwargs_save:
            if len(last_time_urls) == 0:
                self.logger.info(
                    '=== parse_start_response 前回の続きまで再取得完了 (%s)', response.url)
                end_flg = True

        # 終了ページを超えたら終了する。
        if now_page + 1 > end_page:
            end_flg = True

        if end_flg == False:
            # 次のページを読み込む
            # 'https://www.epochtimes.jp/category/100/1.html'

            print('=== 次のページを読み込むよ〜')
            yield scrapy.Request(response.urljoin(
                url_header + str(now_page + 1) + url_footer
            ), callback=self.parse_start_response)
            # elem: WebElement = driver.find_element_by_css_selector(
            #     '.control-nav-next')
            # elem.click()

        print('=== リンク件数 = ', len(urls_list))
        #print('=== とりあえず', urls_list)

        # # リストに溜めたurlをリクエストへ登録する。
        # for url in urls_list:
        #     yield scrapy.Request(response.urljoin(url), callback=self.parse_news)
        # # 次回向けに1ページ目の10件をcrawler_controllerへ保存する情報
        # self._crawl_next_info[self.name][self.start_urls[self._crawl_urls_count]] = {
        #     'urls': urls_list[0:10],
        #     'crawl_start_time': self._crawl_start_time_iso,
        # }

        #self.common_prosses(self.start_urls[self._crawl_urls_count], urls_list)

        self._crawl_urls_count += 1

    # def parse_start_response(self, response: HtmlResponse):
    #     ''' (拡張メソッド)
    #     取得したレスポンスよりDBへ書き込み
    #     '''
    #     # ループ条件
    #     # 1.現在のページ数は、5ページまで（仮）
    #     # 2.前回の1ページ目の記事リンク（10件）まで全て遡りきったら、前回以降に追加された記事は取得完了と考えられるため終了。

    #     pages: dict = self.pages_setting(1, 1)
    #     start_page: int = pages['start_page']
    #     end_page: int = pages['end_page']

    #     driver: WebDriver = response.request.meta['driver']
    #     urls_list: list = []

    #     # 前回からの続きの指定がある場合、前回の１ページ目の30件のURLを取得する。
    #     last_time_urls: list = []
    #     if 'continued' in self.kwargs_save:
    #         last_time_urls: list = self._crawler_controller_recode[
    #             self.name][self.start_urls[self._crawl_urls_count]]['urls']

    #     now_page:int = start_page
    #     while now_page <= end_page:
    #         self.logger.info(
    #             '=== parse_start_response 現在処理中のURL = %s', driver.current_url)

    #         # クリック対象が読み込み完了していることを確認
    #         # next_page_selecter: str = '.control-nav-next[href$="view=page&page=' + \
    #         #     str(now_page + 1) + '&pageSize=10"]'
    #         next_page_selecter: str = '.middle-inner'
    #         WebDriverWait(driver, 10).until(
    #             EC.presence_of_element_located(
    #                 (By.CSS_SELECTOR, next_page_selecter))
    #         )

    #         # 現在のページ内の記事のリンクをリストへ保存
    #         links: list = driver.find_elements_by_css_selector(
    #             '.middle-inner a[href]')
    #         for link in links:
    #             link: WebElement
    #             url: str = urllib.parse.unquote(link.get_attribute("href"))
    #             urls_list.append(url)

    #             # 前回取得したurlが確認できたら確認済み（削除）にする。
    #             if url in last_time_urls:
    #                 last_time_urls.remove(url)

    #         # 前回からの続きの指定がある場合、前回の１ページ目のurlが全て確認できたら前回以降に追加された記事は全て取得完了と考えられるため終了する。
    #         if 'continued' in self.kwargs_save:
    #             if len(last_time_urls) == 0:
    #                 self.logger.info(
    #                     '=== parse_start_response 前回の続きまで再取得完了 (%s)', driver.current_url)
    #                 break

    #         # 次のページを読み込む
    #         # elem: WebElement = driver.find_element_by_css_selector(
    #         #     '.control-nav-next')
    #         # elem.click()

    #         now_page += 1  # 次のページ数

    #     print('=== とりあえず',urls_list)

    #     # # リストに溜めたurlをリクエストへ登録する。
    #     # for url in urls_list:
    #     #     yield scrapy.Request(response.urljoin(url), callback=self.parse_news)
    #     # # 次回向けに1ページ目の10件をcrawler_controllerへ保存する情報
    #     # self._crawl_next_info[self.name][self.start_urls[self._crawl_urls_count]] = {
    #     #     'urls': urls_list[0:10],
    #     #     'crawl_start_time': self._crawl_start_time_iso,
    #     # }

    #     self.common_prosses(self.start_urls[self._crawl_urls_count], urls_list)

    #     self._crawl_urls_count += 1
