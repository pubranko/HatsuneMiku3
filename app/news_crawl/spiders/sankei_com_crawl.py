from datetime import datetime, timedelta
import time
from typing import Union, Any
from news_crawl.spiders.extensions_class.extensions_crawl import ExtensionsCrawlSpider
from scrapy.http import Response
from scrapy_selenium import SeleniumRequest
from scrapy.exceptions import CloseSpider
from dateutil import parser
import scrapy
import sys
from selenium.webdriver.remote.webdriver import WebDriver
from selenium.webdriver.remote.webelement import WebElement
from news_crawl.spiders.common.start_request_debug_file_generate import start_request_debug_file_generate
from bs4 import BeautifulSoup as bs4
from bs4.element import ResultSet

'''
このソースは現在未使用。
'''

class SankeiComCrawlSpider(ExtensionsCrawlSpider):
    name: str = 'sankei_com_crawl'
    allowed_domains: list = ['sankei.com']
    start_urls: list = [
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

    # 廃止された項目だがエラーが出ないようにとりあえず定義しているだけ。
    kwargs_save:dict

    # start_urlsまたはstart_requestの数。起点となるurlを判別するために使う。
    _crawl_urls_count: int = 0

    def __init__(self, *args, **kwargs):
        ''' (拡張メソッド)
        親クラスの__init__処理後に追加で初期処理を行う。
        '''
        super().__init__(*args, **kwargs)

        # 単項目チェック（追加）
        if not 'category_urls' in kwargs:
            raise CloseSpider('引数エラー：当スパイダー(' + self.name +
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
                #errback=self.errback_handle,
            )

    def parse_start_response(self, response: Response):
        ''' (拡張メソッド)
        取得したレスポンスよりDBへ書き込み
        '''
        # ループ条件
        # 1.現在のページ数は、3ページまで（仮）
        # 2.前回の1ページ目の記事リンク（10件）まで全て遡りきったら、前回以降に追加された記事は取得完了と考えられるため終了。
        # ※続きボタン押下してajaxでデータを取得した状態を次のページとして取り扱う。
        page_from, page_to = self.pages_setting(1, 3)

        r:Any = response.request
        driver: WebDriver = r.meta['driver']
        #driver: WebDriver = response.request.meta['driver']
        urls_list: list = []

        # 直近の数分間の指定がある場合
        until_this_time: datetime = self.news_crawl_input.crawling_start_time
        if 'lastmod_recent_time' in self.kwargs_save:
            until_this_time = until_this_time - \
                timedelta(minutes=int(self.kwargs_save['lastmod_recent_time']))
            self.logger.info(
                f'=== parse_start_response : lastmod_recent_timeより計算した時間 {until_this_time.isoformat()}')

        last_time: datetime = datetime.now()  # 型ヒントエラー回避用の初期値
        if 'continued' in self.kwargs_save:
            last_time = parser.parse(
                self._crawl_point[response.url]['latest_lastmod'])
        # 処理中のページ内で、最大のlastmodとurlを記録するエリア。とりあえず初期値には約10年前を指定。
        max_lstmod: datetime = datetime.now().astimezone(
            self.settings['TIMEZONE']) - timedelta(days=3650)
        max_url: str = ''

        page = page_from
        next_page_flg = False
        debug_urls_list = []
        self.logger.info(
            f'=== parse_start_response 現在解析中のpage={page} と URL = {driver.current_url}')
        while page <= page_to:  # 条件はあとで考える

            # Javascript実行が終了するまで最大30秒間待つように指定
            driver.set_script_timeout(60)

            elements = driver.find_elements_by_css_selector(
                '#fusion-app > div > div > section > .story-card-feed .storycard .story-card-flex')
            self.logger.info(
                f'=== parse_start_response ページ内リンク件数 = {len(elements)}')

            urls_list = []
            element: WebElement
            for element in elements:
                # 要素から、url,title,最終更新時間を取得
                url: str = element.find_element_by_css_selector(
                    'h4 a[href]').get_attribute('href')
                title = element.find_element_by_css_selector('h4 a').text
                _ = element.find_element_by_css_selector(
                    '.under-headline time')
                lastmod_str: str = _.get_attribute('datetime')
                lastmod_parse: datetime = parser.parse(lastmod_str).astimezone(
                    self.settings['TIMEZONE'])

                # 最新の記事の時間とurlを記録
                if max_lstmod < lastmod_parse:
                    max_lstmod = lastmod_parse
                    max_url = url

                crwal_flg: bool = True

                if 'lastmod_recent_time' in self.kwargs_save:             # lastmod絞り込み指定あり
                    if lastmod_parse < until_this_time:
                        crwal_flg = False
                        next_page_flg = True
                if 'continued' in self.kwargs_save:
                    if lastmod_parse < last_time:
                        crwal_flg = False
                        next_page_flg = True
                    elif lastmod_parse == last_time \
                            and self._crawl_point[response.url]['latest_url']:
                        crwal_flg = False
                        next_page_flg = True

                debug_urls_list.append(
                    {'loc': url, 'lastmod': lastmod_parse.isoformat()})
                if crwal_flg:
                    # ページ内のURLと更新日時をリストに保存する。
                    urls_list.append(
                        {'loc': url, 'lastmod': lastmod_parse.isoformat()})

            self.logger.info(
                f'=== parse_start_response クロール対象リンク件数 = {len(urls_list)}')

            # 次のページを読み込む必要がなくなった場合
            if next_page_flg:
                self.logger.info(
                    f'=== parse_start_response 指定範囲のリンク取得完了 ({driver.current_url})')
                break

            # 次のページを読み込む
            elem: WebElement = driver.find_element_by_css_selector(
                '.feedPagination')
            elem.click()
            time.sleep(3)  # クリック後、最低2秒の空きを設ける。

            page += 1  # 次のページ数

        # リストに溜めたurlをリクエストへ登録する。
        for _ in urls_list:
            #yield scrapy.Request(response.urljoin(_['loc']), callback=self.parse_news, errback=self.errback_handle)
            yield scrapy.Request(response.urljoin(_['loc']), callback=self.parse_news,)

        # 次回向けに1ページ目の10件をcontrollerへ保存する情報
        self._crawl_point[response.url] = {
            'latest_lastmod': max_lstmod.isoformat(),
            'latest_url': max_url,
            'crawling_start_time': self.news_crawl_input.crawling_start_time.isoformat()
        }

        start_request_debug_file_generate(
            self.name, response.url, debug_urls_list, self.news_crawl_input.debug)

    def pagination_check(self, response: Response) -> ResultSet:
        '''(オーバーライド)
        次ページがあれば、BeautifulSoupのResultSetで返す。
        '''
        soup = bs4(response.text, 'html.parser')
        pagination: ResultSet = soup.select(
            '.pagination > .page-list > li:last-child > a[href]')

        return pagination