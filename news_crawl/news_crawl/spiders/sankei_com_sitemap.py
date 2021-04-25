#import scrapy
from scrapy.spiders import SitemapSpider
from datetime  import datetime,date,timedelta
from dateutil import parser,relativedelta
import re,sys


class SankeiComSitemapSpider(SitemapSpider):
    name = 'sankei_com_sitemap'
    allowed_domains = ['sankei.com']
    sitemap_urls = ['https://www.sankei.com/robots.txt',]

    sitemap_rules = [
        # (r'/politics/news/\d{6}/.+$','parse'),
        # (r'/affairs/news/\d{6}/.+$','parse'),
        # (r'/world/news/\d{6}/.+$','parse'),
        # (r'/economy/news/\d{6}/.+$','parse'),
        # (r'/column/news/\d{6}/.+$','parse'),
    ]

    def sitemap_filter(self, entries):
        '''
        親クラスのSitemapSpiderの同名メソッドをオーバーライド。
        entriesには、サイトマップから取得した情報(loc,lastmodなど）が辞書形式で格納されている。
        サイトマップから取得したurlへrequestを行う前に条件で除外することができる。
        対象のurlの場合、”yield entry”で返すと親クラス側でrequestされる。
        '''
        today = date.today()                     #当日
        #dt_zenjitu = dt_today - timedelta(days=1)   #前日

        today_yymmdd = today.strftime('%y%m%d')
        print(today_yymmdd)

        for entry in entries:
            '''
            ここで当日分に限定してみよう。
            過去の変更分も含めると大変な数になるｗ
            '''
            #print("=== sitemap_filter オーバーライド成功！",entry['lastmod'],entry['loc'],entry.items())
            #dt_lastmod = parser.parse(entry['lastmod'])
            #date_time = datetime.strptime(dt, '%Y-%m-%d')
            #if dt.month == 8 and dt.day == 19:
            #if dt_lastmod.date() >= dt_zenjitu:
            #    yield entry

            url = re.compile(today_yymmdd)
            if url.search(entry['loc']):
                print(entry['loc'])


        sys.exit()


    def parse(self, response):
        pass


