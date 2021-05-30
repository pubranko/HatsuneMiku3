from typing import Pattern
from bs4.element import ResultSet
from news_crawl.spiders.extensions_crawl import ExtensionsCrawlSpider
from scrapy.http.response.html import HtmlResponse
import re
import sys
import scrapy
from bs4 import BeautifulSoup as bs4
import copy


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
    _crawl_next_info: dict = {}
    # 各カテゴリー別に前回取得済みのurls情報を保存
    _last_time_urls: dict = {}

    def __init__(self, *args, **kwargs):
        ''' (拡張メソッド)
        親クラスの__init__処理後に追加で初期処理を行う。
        '''
        super().__init__(*args, **kwargs)

        # 単項目チェック（追加）
        if not 'category_urls' in kwargs:
            sys.exit('引数エラー：当スパイダー(' + self.name +
                     ')の場合、category_urlsは必須です。')

        # 前回のクロール情報を次回向けへ引き継ぎ
        self._crawl_next_info = self._crawler_controller_recode

    def start_requests(self):
        '''
        start_urlsを使わずに直接リクエストを送る。
        '''
        # 開始ページからURLを生成
        pages: dict = self.pages_setting(1, 5)
        start_page: int = pages['start_page']

        # [100, 108, 170, 169, 101]
        category_urls: list = eval(self.kwargs_save['category_urls'])
        urls: list = ['https://www.epochtimes.jp/category/%s' %
                      str(i) + '/' + str(start_page) + '.html' for i in category_urls]

        for url in urls:
            self.start_urls.append(url)
            yield scrapy.Request(url, self.parse_start_response)

    def parse_start_response(self, response: HtmlResponse):
        ''' (拡張メソッド)
        取得したレスポンスよりDBへ書き込み
        '''
        # ループ条件
        # 無制限にクロールしないよう、標準設定を3ページ目までとする。
        pages: dict = self.pages_setting(1, 3)
        start_page: int = pages['start_page']
        end_page: int = pages['end_page']

        urls_list: list = []
        soup = bs4(response.text, 'html.parser')

        self.logger.info(
            '=== parse_start_response 現在処理中のURL = %s', response.url)
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
            self._last_time_urls[url_header] = copy.deepcopy(
                self._crawl_next_info[self.name][url_header]['urls'])

        end_flg = False

        # 現在のページ内の記事のリンクをリストへ保存
        # １件目のアンカーと２件目以降のアンカーを取得。（１ページ目だけ１件目が<table>内に無い）
        # 絶対パスに変換する。その際、リダイレクト先のURLへ直接リンクするよう、ドメイン/pを付与する。
        anchors: ResultSet = soup.select(
            '.category-left > a,table.table.table-hover tr > td[style] > a')
        for anchor in anchors:
            url: str = 'https://www.epochtimes.jp/p' + anchor.get('href')
            urls_list.append(url)
            # 前回からの続きの指定がある場合
            if 'continued' in self.kwargs_save:
                # 前回取得したurlが確認できたら確認済み（削除）にする。
                if url in self._last_time_urls[url_header]:
                    self._last_time_urls[url_header].remove(url)
                # 前回の１ページ目のurlが全て確認できたら前回以降に追加された記事は全て取得完了と考えられるため終了する。
                if len(self._last_time_urls[url_header]) == 0:
                    self.logger.info(
                        '=== parse_start_response 前回の続きまで再取得完了 (%s)', response.url)
                    end_flg = True
                    break

        self.logger.info(
            '=== parse_start_response リンク件数 = %s', len(urls_list))

        self.common_prosses(response.url, urls_list)

        # 終了ページを超えたら終了する。
        if now_page + 1 > end_page:
            end_flg = True

        # 各カテゴリーの最初のページの場合、次回に向け現在クロール中のカテゴリーの情報を更新する。
        # ・1ページ目の10件をcrawler_controllerへ保存
        # ・Keyとなるurlには、http〜各カテゴリー(url_header)までの一部とする。
        if response.url in self.start_urls:
            self._crawl_next_info[self.name][url_header] = {
                'urls': urls_list[0:10],
                'crawl_start_time': self._crawl_start_time_iso,
            }

        # 続きがある場合、次のページを読み込む
        if end_flg == False:
            url_next: str = url_header + str(now_page + 1) + url_footer
            self.logger.info(
                '=== parse_start_response 次のページのURL = %s', url_next)

            yield scrapy.Request(response.urljoin(url_next), callback=self.parse_start_response)

        # リストに溜めたurlをリクエストへ登録する。
        for url in urls_list:
            yield scrapy.Request(response.urljoin(url), callback=self.parse_news)