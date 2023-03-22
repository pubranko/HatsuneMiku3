import urllib.parse
from time import sleep
from typing import Any
import scrapy
from scrapy.http import TextResponse
from scrapy_selenium import SeleniumRequest
from scrapy.exceptions import CloseSpider
from selenium.webdriver.remote.webdriver import WebDriver
from selenium.webdriver.remote.webelement import WebElement
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import NoSuchElementException, StaleElementReferenceException
from news_crawl.spiders.extensions_class.extensions_crawl import ExtensionsCrawlSpider
from news_crawl.spiders.common.start_request_debug_file_generate import start_request_debug_file_generate, LOC as debug_file__LOC, LASTMOD as debug_file__LASTMOD
from news_crawl.spiders.common.urls_continued_skip_check import UrlsContinuedSkipCheck
from news_crawl.items import NewsCrawlItem
from news_crawl.spiders.common.url_pattern_skip_check import url_pattern_skip_check
from shared.login_info_get import login_info_get


class EpochtimesJpCrawlSpider(ExtensionsCrawlSpider):
    name: str = 'epochtimes_jp_crawl'
    allowed_domains: list = ['epochtimes.jp']
    start_urls: list = [
        'https://www.epochtimes.jp/latest',     # 新着
    ]
    _domain_name: str = 'epochtimes_jp'         # 各種処理で使用するドメイン名の一元管理
    _spider_version: float = 1.0

    custom_settings: dict = {
        'DEPTH_LIMIT': 0,
        'DEPTH_STATS_VERBOSE': True,
    }

    # _crawl_point: dict = {}
    # '''次回クロールポイント情報 (ExtensionsCrawlSpiderの同項目をオーバーライド必須)'''

    # rules = (
    #     Rule(LinkExtractor(
    #         allow=(r'/article/')), callback='parse_news'),
    # )

    # seleniumモード
    # selenium_mode: bool = True

    def __init__(self, *args, **kwargs):
        ''' (拡張メソッド)
        親クラスの__init__処理後に追加で初期処理を行う。
        '''
        super().__init__(*args, **kwargs)

        # クロールする対象ページを決定する。デフォルト１〜３。scrapy起動引数に指定がある場合、そちらを使う。
        self.page_from, self.page_to = self.pages_setting(1, 3)
        # URLに埋め込むページ数。現在のページ数を保存するエリア。
        self.page: int = self.page_from
        self.all_urls_list: list = []

        self.url_continued = UrlsContinuedSkipCheck(
            self._crawl_point, self.start_urls[0], self.news_crawl_input.continued)

        # start_urlsを再構築
        # 例）https://www.epochtimes.jp/latest
        #     -> https://www.epochtimes.jp/latest/1, https://www.epochtimes.jp/latest/2
        # page_range = range(self.start_page, self.end_page + 1)
        # self.start_urls = [f'{self.start_urls[0]}/{p}' for p in page_range]

    def start_requests(self):
        ''' '''
        if self.selenium_mode:
            for url in self.start_urls:
                yield SeleniumRequest(
                    url=url,
                    callback=self.parse_start_response_selenium)
        else:
            for url in self.start_urls:
                yield scrapy.Request(
                    url=url,
                    callback=self.parse_start_response)

    def parse_start_response(self, response: TextResponse):
        ''' (拡張メソッド)
        取得したレスポンスよりDBへ書き込み
        '''
        while self.page <= self.page_to:
            self.logger.info(
                f'=== parse_start_response 現在解析中のURL = {response.url}')

            next_page_url = f'{self.start_urls[0]}/{self.page + 1}'

            # ページ内の対象urlを抽出
            links = response.css(
                f'.main_content > .left_col > .posts_list .post_title > a[href]::attr(href)').getall()
            self.logger.info(
                f'=== ページ内の記事件数 = {len(links)}')
            # ページ内記事は通常30件。それ以外の場合はワーニングメール通知（環境によって違うかも、、、）
            if not len(links) == 30:
                self.logger.warning(
                    f'=== parse_start_response 1ページ内で取得できた件数が想定の30件と異なる。確認要。 ( {len(links)} 件)')

            for link in links:
                # 相対パスの場合絶対パスへ変換。また%エスケープされたものはUTF-8へ変換
                url: str = urllib.parse.unquote(response.urljoin(link))
                self.all_urls_list.append(
                    {debug_file__LOC: url, debug_file__LASTMOD: ''})
                # 前回からの続きの指定がある場合、
                # 前回取得したurlが確認できたら確認済み（削除）にする。

                if self.url_continued.skip_check(url):
                    pass
                elif url_pattern_skip_check(url, self.news_crawl_input.url_pattern):
                    pass
                else:
                    # クロール対象のURL情報を保存
                    self.crawl_urls_list.append(
                        {self.CRAWL_URLS_LIST__LOC: url, self.CRAWL_URLS_LIST__LASTMOD: '', self.CRAWL_URLS_LIST__SOURCE_URL: response.url})
                    self.crawl_target_urls.append(url)

            # debug指定がある場合、現ページの３０件をデバック用ファイルに保存
            start_request_debug_file_generate(
                self.name, response.url, self.all_urls_list[-30:], self.news_crawl_input.debug)

            # 前回からの続きの指定がある場合、前回の10件のurlが全て確認できたら前回以降に追加された記事は全て取得完了と考えられるため終了する。
            if self.url_continued.skip_flg == True:
                self.logger.info(
                    f'=== parse_start_response 前回の続きまで再取得完了 ({response.url})', )
                self.page = self.page_to + 1
                break

            # 次のページを読み込む
            self.page += 1
            if self.page <= self.page_to:
                # 要素を表示するようスクロールしてクリック
                yield scrapy.Request(url=next_page_url, callback=self.parse_start_response)

        # リスト(self.urls_list)に溜めたurlをリクエストへ登録する。
        for _ in self.crawl_urls_list:
            yield scrapy.Request(response.urljoin(_[self.CRAWL_POINT__LOC]), callback=self.parse_news,)
        # 次回向けに1ページ目の10件をcontrollerへ保存する
        self._crawl_point[self.start_urls[0]] = {
            self.CRAWL_POINT__URLS: self.all_urls_list[0:self.url_continued.check_count],
            self.CRAWL_POINT__CRAWLING_START_TIME: self.news_crawl_input.crawling_start_time
        }

    def parse_start_response_selenium(self, response: TextResponse):
        ''' (拡張メソッド)
        取得したレスポンスよりDBへ書き込み(selenium版)
        '''
        r: Any = response.request
        driver: WebDriver = r.meta['driver']
        driver.set_page_load_timeout(60)
        driver.implicitly_wait(60)
        driver.set_script_timeout(60)

        # ログイン操作
        # ログインフォーム部のiframe内に入る
        iframe: WebElement = WebDriverWait(driver, 60).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, '#login_wrapper > iframe')))
        driver.switch_to.frame(iframe)

        # ログイン情報を取得する。
        try:
            yaml_file = login_info_get()
            yaml_file[self.allowed_domains[0]]['user']
            yaml_file[self.allowed_domains[0]]['password']
        except Exception as e:
            self.logger.critical(
                f'指定したYAMLファイルがない、またはファイルの中よりユーザー・パスワードが取得できませんでした。{e}')
            raise CloseSpider()
        else:
            user = yaml_file[self.allowed_domains[0]]['user']
            password = yaml_file[self.allowed_domains[0]]['password']

        try:
            elem: WebElement = driver.find_element_by_css_selector(
                '#mypage')  # ログイン前なら存在
        except NoSuchElementException:  # 既にログイン中ならpass
            pass
        else:
            # ログインウインドウを開く
            elem.click()
            # iframeから出る
            driver.switch_to.default_content()

            # ログインフォームのiframに入る
            iframe2: WebElement = WebDriverWait(driver, 60).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, '#modal-COMMON-content > p > iframe')))
            driver.switch_to.frame(iframe2)

            # ユーザー名（email）入力
            elem: WebElement = WebDriverWait(driver, 60).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, '#ymkemail')))
            elem.send_keys(user)
            # パスワード入力
            elem: WebElement = driver.find_element_by_css_selector(
                '#ymkpassword')
            elem.send_keys(password)
            # ログインボタン押下
            elem: WebElement = driver.find_element_by_css_selector(
                '#ymk-login-btn')
            elem.click()

            # iframeから出る
            driver.switch_to.default_content()
            # ログイン前後でiframの入れ替えが発生する。element名も同じであるため
            # 仕方なく強制スリープでiframeが入れ替わるのを待つ。
            sleep(2)

        # ログイン済みであることを最終チェック
        try:
            iframe3: WebElement = WebDriverWait(driver, 60).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, '#login_wrapper > iframe')))
            driver.switch_to.frame(iframe3)
            WebDriverWait(driver, 60).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, '#ep_user_name')))  # ログイン後なら存在
            driver.switch_to.default_content()
        except NoSuchElementException:
            self.logger.error(
                f'=== ログインできなかったため中止 ({driver.current_url})')

        # 指定ページをループしてクロール対象のurlを収集
        while self.page <= self.page_to:
            self.logger.info(
                f'=== parse_start_response 現在解析中のURL = {driver.current_url}')

            next_page_url = f'{self.start_urls[0]}/{self.page + 1}'
            next_page_element = f'.main_content > .left_col > .pagination > a[href="{next_page_url}"]'
            WebDriverWait(driver, 60).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, next_page_element)))

            # ページ内の対象urlを抽出
            _ = driver.find_elements_by_css_selector(
                f'.main_content > .left_col > .posts_list .post_title > a[href]')
            links: list = [link.get_attribute("href") for link in _]
            self.logger.info(
                f'=== ページ内の記事件数 = {len(links)}')
            # ページ内記事は通常30件。それ以外の場合はワーニングメール通知（環境によって違うかも、、、）
            if not len(links) == 30:
                self.logger.warning(
                    f'=== parse_start_response 1ページ内で取得できた件数が想定の30件と異なる。確認要。 ( {len(links)} 件)')

            for link in links:
                # 相対パスの場合絶対パスへ変換。また%エスケープされたものはUTF-8へ変換
                url: str = urllib.parse.unquote(response.urljoin(link))
                self.all_urls_list.append(
                    {debug_file__LOC: url, debug_file__LASTMOD: ''})
                # 前回からの続きの指定がある場合、前回取得したurlが確認できたらそれ以降のurlは対象外
                # urlパターンの指定がある場合、パターンに合わないurlは対象外
                if self.url_continued.skip_check(url):
                    pass
                elif url_pattern_skip_check(url, self.news_crawl_input.url_pattern):
                    pass
                else:
                    # クロール対象のURL情報を保存
                    self.crawl_urls_list.append(
                        {self.CRAWL_URLS_LIST__LOC: url, self.CRAWL_URLS_LIST__LASTMOD: '', self.CRAWL_URLS_LIST__SOURCE_URL: driver.current_url})
                    self.crawl_target_urls.append(url)

            # debug指定がある場合、現ページの３０件をデバック用ファイルに保存
            #   末尾から３０件と指定しているが、最後のページまで行った場合、前ページ分が混ざるかも、、、どこかで直そう。
            start_request_debug_file_generate(
                self.name, driver.current_url, self.all_urls_list[-30:], self.news_crawl_input.debug)

            # 前回からの続きの指定がある場合、前回の10件のurlが全て確認できたら前回以降に追加された記事は全て取得完了と考えられるため終了する。
            if self.url_continued.skip_flg == True:
                self.logger.info(
                    f'=== parse_start_response 前回の続きまで再取得完了 ({driver.current_url})', )
                self.page = self.page_to + 1
                break

            # 次のページを読み込む
            self.page += 1
            if self.page <= self.page_to:
                # 要素を表示するようスクロールしてクリック
                elem: WebElement = driver.find_element_by_css_selector(
                    next_page_element)
                elem.send_keys(Keys.END)    # endキーを押下して画面最下部へ移動
                elem.click()                # 画面に表示された対象のボタンを押下(表示されていないと押下できない)

        # リスト(self.urls_list)に溜めたクロール対象urlよりリクエストを発行
        for _ in self.crawl_urls_list:
            yield SeleniumRequest(url=response.urljoin(_[self.CRAWL_POINT__LOC]), callback=self.parse_news)
        # 次回向けに1ページ目の10件をcontrollerへ保存する
        self._crawl_point[self.start_urls[0]] = {
            self.CRAWL_POINT__URLS: self.all_urls_list[0:self.url_continued.check_count],
            self.CRAWL_POINT__CRAWLING_START_TIME: self.news_crawl_input.crawling_start_time,
        }
