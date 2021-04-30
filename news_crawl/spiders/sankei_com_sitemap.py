#import scrapy
from logging import NullHandler
from scrapy.spiders import SitemapSpider
from datetime  import datetime,date,timedelta,timezone
from dateutil import parser,relativedelta
import re,sys,pickle,pprint,scrapy
from news_crawl.items import NewsCrawlItem

class SankeiComSitemapSpider(SitemapSpider):
    name = 'sankei_com_sitemap'
    allowed_domains = ['sankei.com']
    sitemap_urls = ['https://www.sankei.com/robots.txt',]

    lastmod_recent_time:int = 0     #直近の15分など。分単位で指定することにしよう。0は制限なしとする。
    start_time:datetime             #Scrapy起動時点の基準となる時間
    recent_time_limit:datetime      #lastmod_recent_timeとnowから計算した制限をかける時間
    url_pattern:str = ''            #指定したパターンをurlに含むもので限定
    continued:bool = False          #前回の続きから(最後に取得したlastmodの日時) ※実装は後回し

    custom_settings = {
        'DEPTH_LIMIT' : 1,
        'DEPTH_STATS_VERBOSE' : True
    }

    # def __init__(self, category=None, *args, **kwargs):
    #     super(SankeiComSitemapSpider, self).__init__(*args, **kwargs)
    #     self.start_urls = ['http://www.example.com/categories/%s' % category]
    def __init__(self, *args, **kwargs):
        super(SankeiComSitemapSpider, self).__init__(*args, **kwargs)
        if 'lastmod_recent_time' in kwargs:
            self.lastmod_recent_time = int(kwargs['lastmod_recent_time'])
        if 'url_pattern' in kwargs:
            self.url_pattern = kwargs['url_pattern']
        if 'continued' in kwargs:
            self.continued = kwargs['continued']
        '''  どんな引数がいる？
            直近の１時間などの絞り込み。
            sitemapspiderの場合、直近〜の絞り込みしかない？
            一応当日分だけ対象にするかもしれない。
            ※変更分は別途網羅的にやるかも？
        '''

    def sitemap_filter(self, entries):
        '''
        親クラスのSitemapSpiderの同名メソッドをオーバーライド。
        entriesには、サイトマップから取得した情報(loc,lastmodなど）が辞書形式で格納されている。
        サイトマップから取得したurlへrequestを行う前に条件で除外することができる。
        対象のurlの場合、”yield entry”で返すと親クラス側でrequestされる。
        '''

        self.start_time = datetime.now().astimezone(self.settings['TIMEZONE'])       #当日
        self.recent_time_limit = self.start_time - timedelta(minutes=self.lastmod_recent_time)

        for entry in entries:   #entryの使い方：entry['lastmod'],entry['loc'],entry.items()
            '''
            引数に絞り込み指定がある場合、その条件を満たす場合のみ対象とする。
            '''
            crwal_flg :bool = True
            if self.lastmod_recent_time != 0:
                date_lastmod = parser.parse(entry['lastmod']).astimezone(self.settings['TIMEZONE'])
                if date_lastmod < self.recent_time_limit:
                    crwal_flg = False
            if self.url_pattern != '':
                pattern = re.compile(self.url_pattern)
                if pattern.search(entry['loc']) == None:
                    crwal_flg = False

            if crwal_flg: yield entry

    def parse(self, response):
        '''
        CallBack関数として使用。取得したレスポンスより、次のurlを生成し追加している。
        '''
        print('=== parse :',response.url)

        response_time = datetime.now()
        yield NewsCrawlItem(
            #_id=(str(self.allowed_domains[0])+'@'+str(now).replace(' ','@',)),
            url=response.url,
            response_time=response_time,
            response_headers=pickle.dumps(response.headers),
            response_body=pickle.dumps(response.body),
        )   #ヘッダーとボディーは文字列が長くログが読めなくなるため、items.pyのNewsclipItemに細工して文字列を短縮。


