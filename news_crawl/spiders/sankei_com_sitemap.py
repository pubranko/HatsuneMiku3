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

    lastmod_range_from = None
    lastmod_range_to = None

    lastmod_recent_time = None  #直近の15分など。分単位で指定することにしよう。
    continued = None    #前回の続きから(最後に取得したlastmodの日時)
    pattern = None      #指定したパターンをurlに含むもので限定

    # def __init__(self, category=None, *args, **kwargs):
    #     super(SankeiComSitemapSpider, self).__init__(*args, **kwargs)
    #     self.start_urls = ['http://www.example.com/categories/%s' % category]
    def __init__(self, *args, **kwargs):
        super(SankeiComSitemapSpider, self).__init__(*args, **kwargs)
        if 'lastmod_range_from' in kwargs:
            self.lastmod_range_from = parser.parse(kwargs['lastmod_range_from']) #.astimezone(timezone.utc)
        if 'lastmod_range_to' in kwargs:
            self.lastmod_range_to = kwargs['lastmod_range_to']
        '''  どんな引数がいる？
            直近の１時間などの絞り込み。
            sitemapspiderの場合、直近〜の絞り込みしかない？
            一応当日分だけ対象にするかもしれない。
            ※変更分は別途網羅的にやるかも？
        '''

    # sitemap_rules = [
    #     # (r'/politics/news/\d{6}/.+$','parse'),
    #     # (r'/affairs/news/\d{6}/.+$','parse'),
    #     # (r'/world/news/\d{6}/.+$','parse'),
    #     # (r'/economy/news/\d{6}/.+$','parse'),
    #     # (r'/column/news/\d{6}/.+$','parse'),
    # ]

    def sitemap_filter(self, entries):
        '''
        親クラスのSitemapSpiderの同名メソッドをオーバーライド。
        entriesには、サイトマップから取得した情報(loc,lastmodなど）が辞書形式で格納されている。
        サイトマップから取得したurlへrequestを行う前に条件で除外することができる。
        対象のurlの場合、”yield entry”で返すと親クラス側でrequestされる。
        '''
        print("===========")
        print(self.settings['TIMEZONE'])

        #dt.replace(tzinfo=tz)
        self.lastmod_range_from.replace(tzinfo=self.settings['TIMEZONE'])
        print(self.lastmod_range_from)

        today = datetime.now().astimezone(self.settings['TIMEZONE'])       #当日
        #zenjitu = today - timedelta(days=1)   #前日
        test_limit_time = today - timedelta(hours=1)
        today_yymmdd = today.strftime('%y%m%d')

        for entry in entries:
            '''
            ここで当日分に限定してみよう。
            過去の変更分も含めると大変な数になるｗ
            '''
            #print("=== sitemap_filter オーバーライド成功！",entry['lastmod'],entry['loc'],entry.items())
            #date_lastmod = parser.parse(entry['lastmod']).astimezone(timezone.utc)
            date_lastmod = parser.parse(entry['lastmod']).astimezone(self.settings['TIMEZONE'])
            #print([date_lastmod,test_limit_time])
            if date_lastmod >= test_limit_time:
                #print(entry['loc'])
                yield entry

            # url = re.compile(today_yymmdd)
            # if url.search(entry['loc']):
            #     #print(entry['loc'])
            #     yield entry

    def parse(self, response):
        '''
        CallBack関数として使用。取得したレスポンスより、次のurlを生成し追加している。
        '''
        print('=== parse :',response.url)

        now = datetime.now()
        yield NewsCrawlItem(
            #_id=(str(self.allowed_domains[0])+'@'+str(now).replace(' ','@',)),
            url=response.url,
            response_time=now,
            response_headers=pickle.dumps(response.headers),
            response_body=pickle.dumps(response.body),
        )   #ヘッダーとボディーは文字列が長くログが読めなくなるため、items.pyのNewsclipItemに細工して文字列を短縮。


