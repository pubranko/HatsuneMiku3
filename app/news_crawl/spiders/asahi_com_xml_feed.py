import re,scrapy
from datetime import datetime, timedelta
from dateutil import parser
from scrapy.selector.unified import Selector
from scrapy.http.response.xml import XmlResponse
from news_crawl.spiders.extensions_class.extensions_xml_feed import ExtensionsXmlFeedSpider

'''
現在このソースは未使用。
XmlFeedSpiderの使用例のサンプルとし保存しているのみ。
'''

class AsahiComXmlFeedSpider(ExtensionsXmlFeedSpider):
    name: str = 'asahi_com_xml_feed'
    allowed_domains: list = ['asahi.com']
    start_urls: list = [
        # 'http://www.asahi.com/sitemap.xml',
        'https://www.asahi.com/sitemap/sitemap_national.xml',
        'https://www.asahi.com/sitemap/sitemap_business.xml',
        'https://www.asahi.com/sitemap/sitemap_politics.xml',
        'https://www.asahi.com/sitemap/sitemap_sports.xml',
        'https://www.asahi.com/sitemap/sitemap_international.xml',
        'https://www.asahi.com/sitemap/sitemap_culture.xml',
        'https://www.asahi.com/sitemap/sitemap_science.xml',
        'https://www.asahi.com/sitemap/sitemap_obituaries.xml',
    ]
    _spider_version: float = 0.0          # spiderのバージョン。継承先で上書き要。
    _domain_name = 'asahi_com'         # 各種処理で使用するドメイン名の一元管理。継承先で上書き要。

    # 廃止された項目だがエラーが出ないようにとりあえず定義しているだけ。
    kwargs_save:dict


    def __init__(self, *args, **kwargs):
        ''' (拡張メソッド)
        親クラスの__init__処理後に追加で初期処理を行う。
        '''
        super().__init__(*args, **kwargs)

    def parse_node(self, response: XmlResponse, node: Selector):
        '''
        itertagに指定したタグ単位に順次処理を実施
        '''
        # 直近の数分間の指定がある場合
        # until_this_time: datetime = self._crawling_start_time
        until_this_time: datetime = self.news_crawl_input.crawling_start_time
        if 'lastmod_recent_time' in self.kwargs_save:
            until_this_time = until_this_time - \
                timedelta(minutes=int(self.kwargs_save['lastmod_recent_time']))

        # 前回からの続きの指定がある場合
        _last_time: datetime = datetime.now()  # 型ヒントエラー回避用の初期値
        if 'continued' in self.kwargs_save:
            _last_time = parser.parse(
                self._crawl_point[response.url]['latest_lastmod'])

        # itertagに指定したタグの中身を取得
        node_text: str = node.getall()[0]

        # locタグよりurlを取得
        pattern = re.compile(r'<loc>.*</loc>')
        loc = pattern.findall(node_text)[0]
        pattern = re.compile(r'^<loc>')
        loc = pattern.sub('', loc)
        pattern = re.compile(r'</loc>$')
        loc = pattern.sub('', loc)

        # news:publication_dateタグより最終更新日時を取得
        pattern = re.compile(
            r'<news:publication_date>.*</news:publication_date>')
        lastmod_str: str = pattern.findall(node_text)[0]
        pattern = re.compile(r'^<news:publication_date>')
        lastmod_str = pattern.sub('', lastmod_str)
        pattern = re.compile(r'</news:publication_date>$')
        lastmod_str = pattern.sub('', lastmod_str)
        lastmod = parser.parse(lastmod_str).astimezone(
            self.settings['TIMEZONE'])

        self._entries.append({'loc':loc, 'lastmod':lastmod_str})
        self._request_list.append(loc)

        if self._max_lstmod < lastmod_str:
            self._max_lstmod = lastmod_str
            self._max_url = loc

        _crwal_flg: bool = True
        _date_lastmod = lastmod

        if 'url_pattern' in self.kwargs_save:   # url絞り込み指定あり
            pattern = re.compile(self.kwargs_save['url_pattern'])
            if pattern.search(loc) == None:
                _crwal_flg = False
        if 'lastmod_recent_time' in self.kwargs_save:             # lastmod絞り込み指定あり
            if _date_lastmod < until_this_time:
                _crwal_flg = False
        if 'continued' in self.kwargs_save:
            if _date_lastmod < _last_time:
                _crwal_flg = False
            elif _date_lastmod == _last_time \
                    and self._crawl_point[response.url]['latest_url']:
                _crwal_flg = False

        if _crwal_flg:
            #yield scrapy.Request(response.urljoin(loc), callback=self.parse_news, errback=self.errback_handle)
            yield scrapy.Request(response.urljoin(loc), callback=self.parse_news,)
            self._xml_extract_count += 1
