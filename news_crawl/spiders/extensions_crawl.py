from bs4.element import ResultSet
from scrapy.http import Response
from typing import Any, Tuple
from scrapy.spiders import CrawlSpider, Rule
from datetime import datetime
import pickle
import sys
import re
import os
from news_crawl.items import NewsCrawlItem
from news_crawl.models.mongo_model import MongoModel
from news_crawl.models.crawler_controller_model import CrawlerControllerModel
from datetime import datetime, timedelta
from dateutil import parser
from news_crawl.settings import TIMEZONE

from scrapy.linkextractors import LinkExtractor
from bs4 import BeautifulSoup as bs4


class ExtensionsCrawlSpider(CrawlSpider):
    '''
    CrawlSpiderの機能を拡張したクラス。
    Override → __init__(),parse(),closed()
    (前提)
    name, allowed_domains, start_urls, _domain_name, spider_versionの値は当クラスを継承するクラスで設定すること。
    '''

    name: str = 'extension_crawl'                                 # 継承先で上書き要。
    allowed_domains: list = ['sample.com']                          # 継承先で上書き要。
    start_urls: list = ['https://www.sample.com/crawl.html', ]   # 継承先で上書き要。
    custom_settings: dict = {
        'DEPTH_LIMIT': 1,
        'DEPTH_STATS_VERBOSE': True
    }
    spider_version: float = 0.0          # spiderのバージョン。継承先で上書き要。
    _extensions_crawl_version: float = 1.0         # 当クラスのバージョン

    # 引数の値保存
    # MongoDB関連
    mongo: MongoModel                   # MongoDBへの接続を行うインスタンスをspider内に保持。pipelinesで使用。
    _crawler_controller_recode: Any     # crawler_controllerコレクションから取得した対象ドメインのレコード
    # スパイダーの挙動制御関連、固有の情報など
    _crawl_start_time: datetime         # Scrapy起動時点の基準となる時間
    _crawl_start_time_iso: str          # Scrapy起動時点の基準となる時間(ISOフォーマットの文字列)
    _domain_name = 'sample_com'         # 各種処理で使用するドメイン名の一元管理。継承先で上書き要。

    # start_urlsに複数のurlを指定した場合、どのurlの処理中か判別できるようにカウントする。
    _crawl_start_urls_count: int = 0
    # crawler_controllerコレクションへ書き込むレコードのdomain以降のレイアウト雛形。※最上位のKeyのdomainはサイトの特性にかかわらず固定とするため。
    _crawl_next_info: dict = {name: {}, }

    kwargs_save: dict                    # 取得した引数を保存

    rules = (
        Rule(LinkExtractor(
            allow=(r'/article/')), callback='parse_news'),
    )

    def __init__(self, *args, **kwargs):
        ''' (拡張メソッド)
        親クラスの__init__処理後に追加で初期処理を行う。
        '''
        super().__init__(*args, **kwargs)
        self.mongo = MongoModel()
        self.kwargs_save: dict = kwargs

        # 前回のドメイン別のクロール結果を取得
        _crawler_controller = CrawlerControllerModel(self.mongo)
        self._crawler_controller_recode = _crawler_controller.find_one(
            {'domain': self._domain_name})
        self.logger.info(
            '=== __init__ : crawler_controllerにある前回情報 \n %s', self._crawler_controller_recode)

        # 引数チェック・保存
        self.__argument_check(
            self._domain_name, self._crawler_controller_recode, *args, **kwargs)

        # クロール開始時間
        self._crawl_start_time = datetime.now().astimezone(
            TIMEZONE)
        self._crawl_start_time_iso = self._crawl_start_time.isoformat()

    def __argument_check(self, domain_name: str, crawler_controller_recode: Any, *args, **kwargs) -> None:
        ''' (拡張メソッド)
        引数のチェックを行う。
        '''
        ### 単項目チェック ###
        if 'debug' in kwargs:
            if kwargs['debug'] == 'Yes':
                self.logger.info('=== debugモード ON: %s', self.name)
                # デバック用のファイルを初期化
                # _ = open('debug/debug_start_url(' + self.name + ').txt', 'w')
                # _.close()
                path = os.path.join(
                    'news_crawl', 'debug', 'debug_start_url(' + self.name + ').txt')
                with open(path, 'w') as _:
                    pass
            else:
                sys.exit('引数エラー：debugに指定できるのは"Yes"のみです。')
        if 'url_term_days' in kwargs:
            if not kwargs['url_term_days'].isdecimal():
                sys.exit('引数エラー：url_term_daysは数字のみ使用可。日単位で指定してください。')
            elif kwargs['url_term_days'] == 0:
                sys.exit('引数エラー：url_term_daysは0日の指定は不可です。1日以上を指定してください。')
        if 'lastmod_recent_time' in kwargs:
            if not kwargs['lastmod_recent_time'].isdecimal():
                sys.exit('引数エラー：lastmod_recent_timeは数字のみ使用可。分単位で指定してください。')
            elif kwargs['lastmod_recent_time'] == 0:
                sys.exit('引数エラー：lastmod_recent_timeは0分の指定は不可です。')

        ### 項目関連チェック ###
        # pass

    def parse_start_url(self, response: Response):
        '''
        start_urls自体のレスポンスの処理
        '''
        # soup = bs4(response.body, 'lxml')
        # links = soup.find_all('a', attrs={'href', re.compile(".*")})
        # _ = open('debug/debug_start_url(' + self.name + ').txt', 'a')
        # for link in links:
        #     print(link['href'])
        #     _.write(
        #         self.start_urls[self._crawl_urls_count] + ',' + link['href'] + ',')
        # _.close()

        self.common_prosses(self.start_urls[self._crawl_urls_count], response)

        self._crawl_urls_count += 1  # 次のurl用にカウントアップ
        # soup.find_all(href=re.compile("news.yahoo.co.jp/pickup"))
        # soup.find_all("a", class_="link", href="/link")
        # soup.find_all("a", attrs={"class": "link", "href": "/link"})
        # soup.find_all(class_="link", href="/link")
        # soup.find_all(attrs={"class": "link", "href": "/link"})
        # soup.find_all(re.compile("^b")) #bで始まるタグ
        # soup.find_all("a", text=re.compile("hello"))
        # print(a4.a.get("href"),'\n')
        # print(type(_)) #bs4.element.Tag'
        #a = response.css('a[href]')
        # a1 = soup.find('a')
        # a2 = soup.find_all('a')
        # a3 = soup.a.get("href")
        # response
        # print()

        # self.parse_item(response)

    def common_prosses(self, start_urls, response: Response):
        ''' (拡張メソッド)
        デバックモードが指定された場合、entriesと元となったsitemapのurlをdebug_entries.txtへ出力する。
        '''
        if 'debug' in self.kwargs_save:         # sitemap調査用。debugモードの場合のみ。
            soup = bs4(response.body, 'lxml')

            links:ResultSet = soup.find_all('a')
            path:str = os.path.join(
                'news_crawl', 'debug', 'debug_start_url(' + self.name + ').txt')
            _fullpath:str = ''
            with open(path, 'a') as _:
                for link in links:
                    if link['href'][0:2] == '//':
                        _fullpath = 'https:' + link['href']
                    elif link['href'][0:1] == '/':
                        _fullpath = 'https://jp.reuters.com' + link['href']
                    else:
                        _fullpath = link['href']

                    _.write(_fullpath + '\n')

    def term_days_Calculation(self, crawl_start_time: datetime, term_days: int, date_pattern: str) -> list:
        ''' (拡張メソッド)
        クロール開始時刻(crawl_start_time)を起点に、期間(term_days)分の日付リスト(文字列)を返す。
        日付の形式(date_pattern)には、datetime.strftimeのパターンを指定する。
        '''
        return [(crawl_start_time - timedelta(days=i)).strftime(date_pattern) for i in range(term_days)]

    def parse_news(self, response: Response):
        ''' (拡張メソッド)
        取得したレスポンスよりDBへ書き込み
        '''
        _info = self.name + ':' + str(self.spider_version) + ' / ' \
            + 'extensions_crawl:' + str(self._extensions_crawl_version)

        yield NewsCrawlItem(
            url=response.url,
            response_time=datetime.now().astimezone(self.settings['TIMEZONE']),
            response_headers=pickle.dumps(response.headers),
            response_body=pickle.dumps(response.body),
            spider_version_info=_info
        )

    def closed(self, spider):
        '''
        spider終了時、次回クロール向けの情報をcrawler_controllerへ記録する。
        '''
        self.logger.info('=== Spider closed: %s', self.name)
        # crawler_controller = CrawlerControllerModel(self.mongo)
        # crawler_controller.update(
        #     {'domain': self._domain_name},
        #     {'domain': self._domain_name,
        #      self.name: self._crawl_next_info[self.name],
        #      },
        # )

        # _ = crawler_controller.find_one(
        #     {'domain': self._domain_name})
        # self.logger.info(
        #     '=== closed : crawler_controllerに次回情報を保存 \n %s', _)

        self.mongo.close()
