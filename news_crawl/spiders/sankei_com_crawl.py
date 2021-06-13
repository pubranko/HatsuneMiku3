from news_crawl.spiders.extensions_crawl import ExtensionsCrawlSpider
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import Rule
from scrapy.http import Response
from scrapy_selenium import SeleniumRequest
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

    #続きボタン、ページありのサンプル
    #https://www.sankei.com/article/20210612-63TRGI366BPD5F2QJOR5EVC7RI/

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
        print('=== ',type(self.kwargs_save['category_urls']))
        print('=== ',self.kwargs_save['category_urls'])
        category_str :str = self.kwargs_save['category_urls']
        category_urls: list = category_str.lstrip('[').rstrip(']').split(',')
        print('=== ',category_urls)

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
        pages: dict = self.pages_setting(1, 3)
        start_page: int = pages['start_page']
        end_page: int = pages['end_page']

        driver: WebDriver = response.request.meta['driver']
        urls_list: list = []

        # 前回からの続きの指定がある場合、前回の１０件のURLを取得する。
        last_time_urls: list = []
        if 'continued' in self.kwargs_save:
            last_time_urls = [
                _['loc'] for _ in self._next_crawl_info[self.name][response.url]['urls']]

        #<a href="/article/20210612-VBUAPUDHTFI2LMFQ6XBJMCFKVE/">香港民主派の周庭氏出所　刑期短縮</a>
        #<div class="story-card-feed | grid"><div class="grid__col--lg-12 grid__col--md-12 grid__col--sm-12 "><article class="storycard border-bottom border-color-gray flex margin-bottom padding-bottom small-size" data-tb-region-item="true"><figure class="margin-left order-2"><a href="/article/20210612-NI6D6RA5EBPN3PSWVHR7WS3XRY/"><img src="https://sankei-sankei-prod.cdn.arcpublishing.com/resizer/_j1q5F-gx8ZUoOYAUTekhpfP_VU=/120x80/smart/cloudfront-ap-northeast-1.images.arcpublishing.com/sankei/B7TN5NBXZFMPHC23NADEAUU5QM.jpg" alt="新型コロナウイルスワクチン大規模接種センターの東京会場に向かう人たち＝１０日午後、東京・大手町" class="width-full"></a></figure><div class="order-1 story-card-flex flex-1 text-content"><h4 class="headline color-gray-80 margin-none"><a href="/article/20210612-NI6D6RA5EBPN3PSWVHR7WS3XRY/">電話でも受け付け　予約枠活用呼びかけ　自衛隊接種会場</a></h4><div class="under-headline margin-bottom"><time datetime="2021-06-12T03:07:02.147Z" class="storycard__time margin-right">6/12 12:07</time><a href="/life" class="article-section color-blue-primary hidden-mobile margin-right">ライフ</a><a href="/life/body" class="article-section color-blue-primary hidden-mobile margin-right">からだ</a><span class="color-accent-red">New</span></div></div></article><article class="storycard border-bottom border-color-gray flex margin-bottom padding-bottom small-size" data-tb-region-item="true"><figure class="margin-left order-2"><a href="/article/20210612-XXMZP4TP5VNS3JLQYW7UC5BJG4/"><img src="https://sankei-sankei-prod.cdn.arcpublishing.com/resizer/n-yIAiRCAq8C6lE7avLzGBnEAEs=/120x80/smart/cloudfront-ap-northeast-1.images.arcpublishing.com/sankei/WAOOPNYQF5O2NE3S2IKLB5GTZQ.jpg" alt="天安門事件から３２年を迎えた朝の天安門周辺＝４日、北京（共同）" class="width-full"></a></figure><div class="order-1 story-card-flex flex-1 text-content"><h4 class="headline color-gray-80 margin-none"><a href="/article/20210612-XXMZP4TP5VNS3JLQYW7UC5BJG4/">【モンテーニュとの対話　随想録を読みながら】毛にあって習にな…</a></h4><div class="under-headline margin-bottom"><time datetime="2021-06-12T03:00:00Z" class="storycard__time margin-right">6/12 12:00</time><a href="/life" class="article-section color-blue-primary hidden-mobile margin-right">ライフ</a><a href="/life/arts" class="article-section color-blue-primary hidden-mobile margin-right">学術・アート</a><span class="color-accent-red">New</span></div></div></article><article class="storycard border-bottom border-color-gray flex margin-bottom padding-bottom small-size" data-tb-region-item="true"><figure class="margin-left order-2"><a href="/article/20210612-KVXB7LSMDJH4TKEJHLXNZB4CF4/"><img src="https://sankei-sankei-prod.cdn.arcpublishing.com/resizer/zto8gwG1jj-lqcLhMN7j0QUM3do=/120x80/smart/cloudfront-ap-northeast-1.images.arcpublishing.com/sankei/XVQKGG3KA5D7LD7QBOSUMQS2NU.jpg" alt="インテリチームとして参戦した乃木坂４６の（左から）秋元真夏、久保史緒里、北川悠理、賀喜遥香（Ｃ）フジテレビ" class="width-full"></a></figure><div class="order-1 story-card-flex flex-1 text-content"><h4 class="headline color-gray-80 margin-none"><a href="/article/20210612-KVXB7LSMDJH4TKEJHLXNZB4CF4/">乃木坂・秋元ら選抜インテリメンバーで「ネプリーグ」参戦</a></h4><div class="under-headline margin-bottom"><time datetime="2021-06-12T03:00:00Z" class="storycard__time margin-right">6/12 12:00</time><a href="/entertainments" class="article-section color-blue-primary hidden-mobile margin-right">エンタメ</a><span class="color-accent-red">New</span></div></div></article></div></div>
        #story-card-feed

        while start_page <= end_page:   #条件はあとで考える
            self.logger.info(
                '=== parse_start_response 現在解析中のURL = %s', driver.current_url)

            # # クリック対象が読み込み完了していることを確認   例）href="?view=page&amp;page=2&amp;pageSize=10"
            start_page += 1  # 次のページ数
            # next_page_selecter: str = '.control-nav-next[href$="view=page&page=' + \
            #     str(start_page) + '&pageSize=10"]'
            # WebDriverWait(driver, 10).until(
            #     EC.presence_of_element_located(
            #         (By.CSS_SELECTOR, next_page_selecter))
            # )


            # <section aria-labelledby="main-title" 
            #     class="grid__column-left-content grid__col--lg-8 grid__col--md-12 grid__col--sm-12 grid__col--xs-12">
            #         <h2 class="speech" id="main-title">メインコンテンツ</h2>
            #         <header class="story-card-header blue large margin-bottom-sm padding-bottom">
            #             <h3 class="row margin-none font-bold">速報</h3>
            #         </header>
            #         <div class="story-card-feed | grid">
            #             <div class="grid__col--lg-12 grid__col--md-12 grid__col--sm-12 ">
            #                 <article class="storycard border-bottom border-color-gray flex margin-bottom padding-bottom small-size" data-tb-region-item="true">
            #                     <figure class="margin-left order-2">
            #                         <a href="/article/20210613-VUVFED5PKZPOZH3DTWYVWVDBOI/">
            #                             <img src="h
            #                         </a>
            #                     </figure>
            #                     <div class="order-1 story-card-flex flex-1 text-content">
            #                         <h4 class="headline color-gray-80 margin-none">
            #                             <a href="/article/20210613-VUVFED5PKZPOZH3DTWYVWVDBOI/">【気になる！】コミック『あしあと　ちばてつや追想短編集』</a>
            #                         </h4>
            #                         <div class="under-headline margin-bottom">
            #                             <time datetime="2021-06-13T05:45:00Z" class="storycard__time margin-right">6/13 14:45</time>
            #                             <a href="/life" class="article-section color-blue-primary hidden-mobile margin-right">ライフ</a>
            #                             <a href="/life/books" class="article-section color-blue-primary hidden-mobile margin-right">本</a>
            #                             <span class="color-accent-red">New</span>
            #                         </div>
            #                     </div>
            #                 </article>

            pagination: list = driver.find_elements_by_css_selector(
                '.feedPagination')
            print('=== pagination',len(pagination))
            contents: list = driver.find_elements_by_css_selector(
                'section[aria-labelledby]')
            print('=== contents',len(contents))
            for _ in contents:
                _:WebElement
                print('=== contentsの中身',_)

            # articles = driver.find_elements_by_css_selector(
            #     '.story-card-feed .storycard .story-card-flex')
            articles = driver.find_elements_by_css_selector(
                '#fusion-app > div > div > section > .story-card-feed .storycard .story-card-flex')
            for one_article in articles:
                one_article:WebElement
                _ = one_article.find_element_by_css_selector('h4 a[href]')
                url = _.get_attribute('href')
                article = _.text
                _ = one_article.find_element_by_css_selector('.under-headline time')
                lastmod = _.get_attribute('datetime')
                print('=== ',lastmod,'  ',url,'  ',article)


            # a: list = driver.find_elements_by_css_selector(
            #     '.story-card-feed')
            # for _ in a:
            #     print('=== a',_.get_attribute("class"))
                #b = _.find_element_by_css_selector('.story-card-feed .storycard .under-headline .storycard__time')
                #print('=== b',b.text)


            # <button class="feedPagination | gtm-click button button-primary margin-center margin-top margin-bottom" 
            #     data-gtm-category="contents" 
            #     data-gtm-action="read more" 
            #     data-gtm-label="category article list / {text}">ニュースをもっと見る
            # </button>

            # 現在のページ内の記事のリンクをリストへ保存
            links: list = driver.find_elements_by_css_selector(
                '.story-card-feed .storycard .story-card-flex h4 a[href]')

            self.logger.info(
                '=== parse_start_response リンク件数 = %s', len(links))
            for _ in links:
                print('===',_.get_attribute("href"))
            # ページ内リンクは通常10件。それ以外の場合はワーニングメール通知（環境によって違うかも、、、）
            if not len(links) == 10:
                #self.layout_change_notice(response)
                self.logger.warning(
                    '=== parse_start_response 1ページ内で取得できた件数が想定の10件と異なる。確認要。 ( %s 件)', len(links))

            # for link in links:
            #     link: WebElement
            #     url: str = urllib.parse.unquote(link.get_attribute("href"))
            #     urls_list.append({'loc': url, 'lastmod': ''})

            #     # 前回取得したurlが確認できたら確認済み（削除）にする。
            #     if url in last_time_urls:
            #         last_time_urls.remove(url)

            # # 前回からの続きの指定がある場合、前回の１ページ目のurlが全て確認できたら前回以降に追加された記事は全て取得完了と考えられるため終了する。
            # if 'continued' in self.kwargs_save:
            #     if len(last_time_urls) == 0:
            #         self.logger.info(
            #             '=== parse_start_response 前回の続きまで再取得完了 (%s)', driver.current_url)
            #         break

            # # 次のページを読み込む
            # elem: WebElement = driver.find_element_by_css_selector(
            #     '.control-nav-next')
            # elem.click()

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
