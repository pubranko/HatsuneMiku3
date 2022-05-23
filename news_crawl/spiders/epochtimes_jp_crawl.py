from __future__ import annotations
import re
import urllib.parse
import pickle
from datetime import datetime
<<<<<<< HEAD
from time import sleep
=======
>>>>>>> 09aea8511e3e17405c973148f3d5026c38bd4cd2
from typing import Pattern, Any
from bs4.element import ResultSet
from bs4 import BeautifulSoup as bs4
import scrapy
from scrapy.http import Response
from scrapy.http import TextResponse
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import Rule
from scrapy_selenium import SeleniumRequest
from scrapy.exceptions import CloseSpider
from selenium.webdriver.remote.webdriver import WebDriver
from selenium.webdriver.remote.webelement import WebElement
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import NoSuchElementException,StaleElementReferenceException
from news_crawl.spiders.extensions_class.extensions_crawl import ExtensionsCrawlSpider
from news_crawl.spiders.common.start_request_debug_file_generate import start_request_debug_file_generate
from news_crawl.spiders.common.urls_continued_skip_check import UrlsContinuedSkipCheck
from news_crawl.items import NewsCrawlItem
from news_crawl.spiders.common.url_pattern_skip_check import url_pattern_skip_check
<<<<<<< HEAD
from common_lib.login_info_get import login_info_get

=======
from time import sleep
>>>>>>> 09aea8511e3e17405c973148f3d5026c38bd4cd2

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

    # rules = (
    #     Rule(LinkExtractor(
    #         allow=(r'/article/')), callback='parse_news'),
    # )

    # seleniumモード
    selenium_mode: bool = True

    def __init__(self, *args, **kwargs):
        ''' (拡張メソッド)
        親クラスの__init__処理後に追加で初期処理を行う。
        '''
        super().__init__(*args, **kwargs)

        self.pages: dict = self.pages_setting(1, 3)
        self.start_page: int = self.pages['start_page']
        self.end_page: int = self.pages['end_page']
        self.page: int = self.start_page
        self.all_urls_list: list = []

        self.url_continued = UrlsContinuedSkipCheck(
            self._crawl_point, self.start_urls[0], self.kwargs_save)

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
        取得したレスポンスよりDBへ書き込み(selenium版)
        '''
        while self.page <= self.end_page:
            self.logger.info(
                f'=== parse_start_response 現在解析中のURL = {response.url}')

            next_page_url = f'{self.start_urls[0]}/{self.page + 1}'

            # ページ内の対象urlを抽出
            links = response.css(f'.main_content > .left_col > .posts_list .post_title > a[href]::attr(href)').getall()
            self.logger.info(
                f'=== ページ内の記事件数 = {len(links)}')
            # ページ内記事は通常30件。それ以外の場合はワーニングメール通知（環境によって違うかも、、、）
            if not len(links) == 30:
                self.logger.warning(
                    f'=== parse_start_response 1ページ内で取得できた件数が想定の30件と異なる。確認要。 ( {len(links)} 件)')

            for link in links:
                # 相対パスの場合絶対パスへ変換。また%エスケープされたものはUTF-8へ変換
                url: str = urllib.parse.unquote(response.urljoin(link))
                self.all_urls_list.append({'loc': url, 'lastmod': ''})
                # 前回からの続きの指定がある場合、
                # 前回取得したurlが確認できたら確認済み（削除）にする。
                if self.url_continued.skip_check(url):
                    # クロール対象のURL情報を保存
                    self.crawl_urls_list.append(
                        {'loc': url, 'lastmod': '', 'source_url': response.url})

            # debug指定がある場合、現ページの３０件をデバック用ファイルに保存
            start_request_debug_file_generate(
                self.name, response.url, self.all_urls_list[-30:], self.kwargs_save)

            # 前回からの続きの指定がある場合、前回の10件のurlが全て確認できたら前回以降に追加された記事は全て取得完了と考えられるため終了する。
            if self.url_continued.crwal_flg == False:
                self.logger.info(
                    f'=== parse_start_response 前回の続きまで再取得完了 ({response.url})', )
                self.page = self.end_page + 1
                break

            # 次のページを読み込む
            self.page += 1
            if self.page <= self.end_page:
                # 要素を表示するようスクロールしてクリック
                yield scrapy.Request(url=next_page_url, callback=self.parse_start_response)

        # リスト(self.urls_list)に溜めたurlをリクエストへ登録する。
        for _ in self.crawl_urls_list:
            yield scrapy.Request(response.urljoin(_['loc']), callback=self.parse_news,)
        # 次回向けに1ページ目の10件をcontrollerへ保存する
        self._crawl_point[self.start_urls[0]] = {
            'urls': self.all_urls_list[0:10],
            'crawling_start_time': self._crawling_start_time
        }

    def parse_start_response_selenium(self, response: TextResponse):
        ''' (拡張メソッド)
        取得したレスポンスよりDBへ書き込み(selenium版)
        '''
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 09aea8511e3e17405c973148f3d5026c38bd4cd2
        r: Any = response.request
        driver: WebDriver = r.meta['driver']
        driver.set_page_load_timeout(15)
        driver.implicitly_wait(15)
        driver.set_script_timeout(15)

        ### ログイン操作
        # ログインフォーム部のiframe内に入る
        iframe: WebElement = WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, '#login_wrapper > iframe')))
        driver.switch_to_frame(iframe)

<<<<<<< HEAD
        # ログイン情報を取得する。
        try:
            yaml_file = login_info_get()
            yaml_file[self.allowed_domains[0]]['user']
            yaml_file[self.allowed_domains[0]]['password']
        except Exception as e:
            self.logger.critical(f'指定したYAMLファイルがない、またはファイルの中よりユーザー・パスワードが取得できませんでした。{e}')
            raise CloseSpider()
        else:
            user = yaml_file[self.allowed_domains[0]]['user']
            password = yaml_file[self.allowed_domains[0]]['password']


=======
>>>>>>> 09aea8511e3e17405c973148f3d5026c38bd4cd2
        try:
            elem: WebElement = driver.find_element_by_css_selector('#mypage')       #ログイン前なら存在
        except NoSuchElementException:  #既にログイン中ならpass
            pass
        else:
            # ログインウインドウを開く
            elem.click()
            # iframeから出る
            driver.switch_to.default_content()

            # ログインフォームのiframに入る
            iframe2: WebElement = WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, '#modal-COMMON-content > p > iframe')))
            driver.switch_to_frame(iframe2)

            # ユーザー名（email）入力
            elem: WebElement = WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, '#ymkemail')))
<<<<<<< HEAD
            elem.send_keys(user)
            # パスワード入力
            elem: WebElement = driver.find_element_by_css_selector('#ymkpassword')
            elem.send_keys(password)
=======
            elem.send_keys('ユーザー')
            # パスワード入力
            elem: WebElement = driver.find_element_by_css_selector('#ymkpassword')
            elem.send_keys('パスワード')
>>>>>>> 09aea8511e3e17405c973148f3d5026c38bd4cd2
            # ログインボタン押下
            elem: WebElement = driver.find_element_by_css_selector('#ymk-login-btn')
            elem.click()

            # iframeから出る
            driver.switch_to.default_content()
            # ログイン前後でiframの入れ替えが発生する。element名も同じであるため
            # 仕方なく強制スリープでiframeが入れ替わるのを待つ。
            sleep(2)

        ### ログイン済みであることを最終チェック
        try:
            iframe3: WebElement = WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, '#login_wrapper > iframe')))
            driver.switch_to_frame(iframe3)
            WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, '#ep_user_name'))) #ログイン後なら存在
            driver.switch_to.default_content()
        except NoSuchElementException:
            self.logger.error(
                f'=== ログインできなかったため中止 ({driver.current_url})')

        ### 指定ページをループしてクロール対象のurlを収集
        while self.page <= self.end_page:
            self.logger.info(
                f'=== parse_start_response 現在解析中のURL = {driver.current_url}')
<<<<<<< HEAD
=======

>>>>>>> 09aea8511e3e17405c973148f3d5026c38bd4cd2

            next_page_url = f'{self.start_urls[0]}/{self.page + 1}'
            next_page_element = f'.main_content > .left_col > .pagination > a[href="{next_page_url}"]'
            WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, next_page_element)))

            # ページ内の対象urlを抽出
            _ = driver.find_elements_by_css_selector(
                f'.main_content > .left_col > .posts_list .post_title > a[href]')
            links: list = [link.get_attribute("href") for link in _]
<<<<<<< HEAD
=======
        # 各カテゴリー別に前回取得済みのurls情報を保存
        last_time_urls: list = []

        # ループ条件
        # 無制限にクロールしないよう、標準設定を3ページ目までとする。
        pages: dict = self.pages_setting(1, 3)
        start_page: int = pages['start_page']
        end_page: int = pages['end_page']

        urls_list: list = []
        soup = bs4(response.text, 'html.parser')

        self.logger.info(
            f'=== parse_start_response 現在解析中のURL = {response.url}')
        # 現在の処理中のurlを３分割。（ヘッダー、ページ、フッター）
        pattern: Pattern = re.compile(
            r'https://www.epochtimes.jp/category/[0-9]{3}/')
        _ = pattern.sub('', response.url)
        pattern: Pattern = re.compile(r'\.html')
        now_page: int = int(pattern.sub('', _))

        url_header: str = re.findall(
            r'https://www.epochtimes.jp/category/[0-9]{3}/', response.url)[0]
        url_footer: str = re.findall(r'\.html', response.url)[0]

        # 各カテゴリーの最初のページ、and、前回からの続きの指定がある場合
        # 前回のまでのクロール情報からurlsを各カテゴリー別に保存する。
        if response.url in self.start_urls and 'continued' in self.kwargs_save:
            last_time_urls: list = [
                _['loc'] for _ in self._crawl_point[url_header]['urls']]

        end_flg = False

        # 現在のページ内の記事のリンクをリストへ保存
        # １件目のアンカーと２件目以降のアンカーを取得。（１ページ目だけ１件目が<table>内に無い）
        # 絶対パスに変換する。その際、リダイレクト先のURLへ直接リンクするよう、ドメイン/pを付与する。
        anchors: ResultSet = soup.select(
            '.category-left > a[href],table.table.table-hover tr > td[style] > a[href]')
        # ページ内リンクは通常30件。それ以外の場合はワーニングメール通知
        if not len(anchors) == 30:
            #self.layout_change_notice(response)
            self.logger.warning(
                f'=== parse_start_response 1ページ内で取得できた件数が想定の30件と異なる。確認要。 ( {len(anchors)} 件)')

        for anchor in anchors:
            full_path: str = f'https://www.epochtimes.jp/p{anchor.get("href")}'
            urls_list.append({'loc': full_path, 'lastmod': ''})
            # 前回からの続きの指定がある場合
            if 'continued' in self.kwargs_save:
                # 前回取得したurlが確認できたら確認済み（削除）にする。
                if full_path in last_time_urls:
                    last_time_urls.remove(full_path)
                # 前回の１ページ目のurlが全て確認できたら前回以降に追加された記事は全て取得完了と考えられるため終了する。
                if len(last_time_urls) == 0:
                    self.logger.info(
                        f'=== parse_start_response 前回の続きまで再取得完了 ({response.url})')
                    end_flg = True
                    break

        self.logger.info(
            f'=== parse_start_response リンク件数 = {len(urls_list)}')

        start_request_debug_file_generate(
            self.name, response.url, urls_list, self.kwargs_save)

        # 終了ページを超えたら終了する。
        if now_page + 1 > end_page:
            end_flg = True

        # 各カテゴリーの最初のページの場合、次回に向け現在クロール中のカテゴリーの情報を更新する。
        # ・1ページ目の10件をcontrollerへ保存
        # ・Keyとなるurlには、http〜各カテゴリー(url_header)までの一部とする。
        if response.url in self.start_urls:
            self._crawl_point[url_header] = {
                'urls': urls_list[0:10],
                'crawling_start_time': self._crawling_start_time.isoformat()
            }

        # 続きがある場合、次のページを読み込む
        if end_flg == False:
            url_next: str = url_header + str(now_page + 1) + url_footer
>>>>>>> 9e662a5f4c9fb102137ee28aaf08ae8778c8456f
=======
>>>>>>> 09aea8511e3e17405c973148f3d5026c38bd4cd2
            self.logger.info(
                f'=== ページ内の記事件数 = {len(links)}')
            # ページ内記事は通常30件。それ以外の場合はワーニングメール通知（環境によって違うかも、、、）
            if not len(links) == 30:
                self.logger.warning(
                    f'=== parse_start_response 1ページ内で取得できた件数が想定の30件と異なる。確認要。 ( {len(links)} 件)')

            for link in links:
                # 相対パスの場合絶対パスへ変換。また%エスケープされたものはUTF-8へ変換
                url: str = urllib.parse.unquote(response.urljoin(link))
                self.all_urls_list.append({'loc': url, 'lastmod': ''})
                # 前回からの続きの指定がある場合、前回取得したurlが確認できたらそれ以降のurlは対象外
                # urlパターンの指定がある場合、パターンに合わないurlは対象外
                if not self.url_continued.skip_check(url):
                    pass
                elif url_pattern_skip_check(url, self.kwargs_save):
                    pass
                else:
                    # クロール対象のURL情報を保存
                    self.crawl_urls_list.append(
                        {'loc': url, 'lastmod': '', 'source_url': driver.current_url})

            # debug指定がある場合、現ページの３０件をデバック用ファイルに保存
            start_request_debug_file_generate(
                self.name, driver.current_url, self.all_urls_list[-30:], self.kwargs_save)

            # 前回からの続きの指定がある場合、前回の10件のurlが全て確認できたら前回以降に追加された記事は全て取得完了と考えられるため終了する。
            if self.url_continued.crwal_flg == False:
                self.logger.info(
                    f'=== parse_start_response 前回の続きまで再取得完了 ({driver.current_url})', )
                self.page = self.end_page + 1
                break

            # 次のページを読み込む
            self.page += 1
            if self.page <= self.end_page:
                # 要素を表示するようスクロールしてクリック
                elem: WebElement = driver.find_element_by_css_selector(
                    next_page_element)
                elem.send_keys(Keys.END)    # endキーを押下して画面最下部へ移動
                elem.click()                # 画面に表示された対象のボタンを押下(表示されていないと押下できない)

        ### リスト(self.urls_list)に溜めたクロール対象urlよりリクエストを発行
        for _ in self.crawl_urls_list:
            yield SeleniumRequest(
                url=response.urljoin(_['loc']),
                callback=self.parse_news)
            # yield SeleniumRequest(
            #     url=response.urljoin(_['loc']),
            #     callback=self.parse_news_selenium)
        # 次回向けに1ページ目の10件をcontrollerへ保存する
        self._crawl_point[self.start_urls[0]] = {
            'urls': self.all_urls_list[0:10],
            'crawling_start_time': self._crawling_start_time
        }

    # def parse_news_selenium(self, response: TextResponse):
    #     ''' (拡張メソッド)
    #     取得したレスポンスよりDBへ書き込み
    #     '''
    #     r: Any = response.request
    #     driver: WebDriver = r.meta['driver']

    #     # あとでやる！
    #     #
    #     # pagination: ResultSet = self.pagination_check(response)
    #     # if len(pagination) > 0:
    #     #     self.logger.info(
    #     #         f"=== parse_news 次のページあり → リクエストに追加 : {pagination[0].get('href')}")
    #     #     yield scrapy.Request(response.urljoin(pagination[0].get('href')), callback=self.parse_news)

    #     _info = self.name + ':' + str(self._spider_version) + ' / ' \
    #         + 'extensions_crawl:' + str(self._extensions_crawl_version)

    #     source_of_information: dict = {}
    #     for record in self.crawl_urls_list:
    #         record: dict
    #         if response.url == record['loc']:
    #             source_of_information['source_url'] = record['source_url']
    #             source_of_information['lastmod'] = record['lastmod']

    #     yield NewsCrawlItem(
    #         domain=self.allowed_domains[0],
    #         url=response.url,
    #         response_time=datetime.now().astimezone(self.settings['TIMEZONE']),
    #         response_headers=pickle.dumps(response.headers),
    #         response_body=pickle.dumps(driver.page_source),
    #         spider_version_info=_info,
    #         crawling_start_time=self._crawling_start_time,
    #         source_of_information=source_of_information,
    #     )

