import urllib.parse
import scrapy
import time
from datetime import datetime
from dateutil import parser
import urllib.parse
from typing import Any
from scrapy.http import TextResponse
from scrapy_selenium import SeleniumRequest
from selenium.webdriver.remote.webdriver import WebDriver
from selenium.webdriver.remote.webelement import WebElement
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from news_crawl.spiders.extensions_class.extensions_crawl import ExtensionsCrawlSpider
from news_crawl.spiders.common.start_request_debug_file_generate import start_request_debug_file_generate, LOC as debug_file__LOC, LASTMOD as debug_file__LASTMOD
from news_crawl.spiders.common.urls_continued_skip_check import UrlsContinuedSkipCheck
from news_crawl.spiders.common.url_pattern_skip_check import url_pattern_skip_check


class MainichiJpCrawlSpider(ExtensionsCrawlSpider):
    name: str = 'mainichi_jp_crawl'
    allowed_domains: list = ['mainichi.jp']
    start_urls: list = [
        'https://mainichi.jp/flash/',   # ピックアップ、新着
    ]
    _domain_name: str = 'mainichi_jp'        # 各種処理で使用するドメイン名の一元管理
    _spider_version: float = 1.0

    custom_settings: dict = {
        'DEPTH_LIMIT': 0,
        'DEPTH_STATS_VERBOSE': True,
        'DOWNLOADER_MIDDLEWARES': {
            'news_crawl.scrapy_selenium_custom_middlewares.SeleniumMiddleware': 585,
        },
    }

    # _crawl_point: dict = {}
    # '''次回クロールポイント情報 (ExtensionsCrawlSpiderの同項目をオーバーライド必須)'''

    # rules = (
    #     Rule(LinkExtractor(
    #         allow=(r'/article/')), callback='parse_news'),
    # )
    # seleniumモード
    selenium_mode: bool = True
    # splashモード
    #splash_mode: bool = True

    def __init__(self, *args, **kwargs):
        ''' (拡張メソッド)
        親クラスの__init__処理後に追加で初期処理を行う。
        '''
        super().__init__(*args, **kwargs)

        # クロールする対象ページを決定する。デフォルト１〜３。scrapy起動引数に指定がある場合、そちらを使う。
        self.page_from, self.page_to = self.pages_setting(1, 3)
        self.page: int = self.page_from
        self.all_urls_list: list = []
        self.session_id: str = self.name + datetime.now().isoformat()

        # keyにドット(.)があるとエラーMongoDBがエラーとなるためアンダースコアに置き換え
        self.base_url = str(self.start_urls[0]).replace('.', '_')

        self.url_continued = UrlsContinuedSkipCheck(
            self._crawl_point, self.base_url, self.news_crawl_input.continued)

    def start_requests(self):
        ''' '''
        if self.selenium_mode:
            for url in self.start_urls:
                yield SeleniumRequest(
                    url=url,
                    callback=self.parse_start_response_selenium)

    def parse_start_response_selenium(self, response: TextResponse):
        ''' (拡張メソッド)
        取得したレスポンスよりDBへ書き込み(selenium版)
        '''
        r: Any = response.request
        driver: WebDriver = r.meta['driver']

        number_of_details_in_page: int = 20  # 1ページ内の明細数

        self.logger.info(
            f'=== parse_start_response_selenium 現在解析中のURL = {driver.current_url}')
        driver.set_page_load_timeout(60)
        driver.set_script_timeout(60)

        # 毎日はSPAであるため、まず一覧ページ上で１ページ目から対象ページまでのロードを行わせる。(２ページ目から表示などできないため、、、)
        load_page: int = 1
        while load_page <= self.page_to:

            # 各ページ内の末尾の明細が表示されるまで待機。
            target_article_element = f'#article-list > ul > li:nth-child({number_of_details_in_page * load_page})'
            WebDriverWait(driver, 60).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, target_article_element)))

            target_next_page_element = f'div.main-contents span.link-more'
            WebDriverWait(driver, 60).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, target_next_page_element)))

            # 次のページを読み込む
            elem: WebElement = driver.find_element_by_css_selector(
                target_next_page_element)
            elem.location_once_scrolled_into_view   # 要素までスクロールを移動してボタンをウィンドウに表示させる。
            time.sleep(1)
            elem.click()

            load_page += 1

        # 記事の一覧ページより、各記事へのリンクと最終更新日時を取得
        url_find_elems = driver.find_elements_by_css_selector(
            '#article-list > ul > li > a[href]')
        lastmod_find_elems = driver.find_elements_by_css_selector(
            '#article-list > ul > li > a > div > div.articlelist-detail > div > span.articletag-date')

        # 各記事のリンク（element）よりリンクのみ抽出したリストを生成
        links: list = [link.get_attribute("href") for link in url_find_elems]
        self.logger.info(
            f'=== ページ内の記事件数 = {len(links)}')
        # 上記リストよりnews_crawl起動時のパラメータから対象部分の開始インデックス、終了インデックスを求める。
        start_link: int = number_of_details_in_page * (self.page_from - 1)
        end_link: int = number_of_details_in_page * (self.page_to)
        if end_link > len(links):
            end_link = len(links)   # 万が一取得したリンク数を超える位置にend_linkになってしまった場合の備え。

        # 上記開始〜終了インデックス部分のみ抽出したリスト（クロール対象リンクのリスト）を生成
        select_links: list = links[start_link: end_link]
        # ついでに上記開始〜終了インデックスに合わせて最終更新日時のリストを生成
        select_lastmods: list[datetime] = [ parser.parse(_.text) for _ in lastmod_find_elems[start_link: end_link]]

        self.logger.info(f'=== ページ内の記事件数 = {len(links)}')
        self.logger.info(f'=== 抽出対象の記事件数 = {len(select_links)}')
        # 想定件数とことなる場合はワーニングメール通知（環境によって違うかも、、、）
        assumed_number_of_cases: int = number_of_details_in_page * \
            (self.page_to - self.page_from + 1)
        if not len(select_links) == assumed_number_of_cases:
            self.logger.warning(
                f'=== parse_start_response_selenium ページ内で取得できた件数が想定の{assumed_number_of_cases}件と異なる。確認要。 ( {len(select_links)} 件)')

        for idx, link in enumerate(select_links):
            # 相対パスの場合絶対パスへ変換。また%エスケープされたものはUTF-8へ変換
            url: str = urllib.parse.unquote(response.urljoin(link))
            self.all_urls_list.append(
                {debug_file__LOC: url, debug_file__LASTMOD: select_lastmods[idx]})

            # 前回からの続きの指定がある場合、
            # 前回取得したurlまで確認できたらそれ移行は対象外
            if self.url_continued.skip_check(url):
                pass
            # urlパターンの絞り込みで対象外となった場合
            elif url_pattern_skip_check(url, self.news_crawl_input.url_pattern):
                pass
            else:
                # クロール対象のURL情報を保存
                self.crawl_urls_list.append(
                    {self.CRAWL_URLS_LIST__LOC: url, self.CRAWL_URLS_LIST__LASTMOD: select_lastmods[idx], self.CRAWL_URLS_LIST__SOURCE_URL: driver.current_url})
                self.crawl_target_urls.append(url)

            # 前回からの続きの指定がある場合、前回の5件のurlが全て確認できたら前回以降に追加された記事は全て取得完了と考えられるため終了する。
            if self.news_crawl_input.continued:
                if self.url_continued.skip_flg == False:
                    self.logger.info(
                        f'=== parse_start_response_selenium 前回の続きまで再取得完了 ({driver.current_url})', )
                    self.page = self.page_to + 1
                    break

        # debug指定がある場合、現ページの明細数分をデバック用ファイルに保存
        start_request_debug_file_generate(
            self.name, driver.current_url, self.all_urls_list, self.news_crawl_input.debug)

        # リスト(self.urls_list)に溜めたurlをリクエストへ登録する。
        for _ in self.crawl_urls_list:
            yield scrapy.Request(response.urljoin(_[self.CRAWL_POINT__LOC]), callback=self.parse_news,)
        # 次回向けに1ページ目の5件をcontrollerへ保存する
        self._crawl_point[self.base_url] = {
            self.CRAWL_POINT__URLS: self.all_urls_list[0:self.url_continued.check_count],
            self.CRAWL_POINT__CRAWLING_START_TIME: self.news_crawl_input.crawling_start_time,
        }
