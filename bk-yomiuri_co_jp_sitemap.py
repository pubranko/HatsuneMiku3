from datetime import datetime, timedelta,timezone
from dateutil import parser
import re
from news_crawl.spiders.extensions_sitemap import ExtensionsSitemapSpider


class YomiuriCoJpSitemapSpider(ExtensionsSitemapSpider):
    name:str = 'yomiuri_co_jp_sitemap'
    allowed_domains:list = ['yomiuri.co.jp']
    sitemap_urls:list = []
    _domain_name: str = 'yomiuri_co_jp'        # 各種処理で使用するドメイン名の一元管理
    spider_version: float = 1.0

    # sitemap_urlsに複数のサイトマップを指定した場合、その数だけsitemap_filterが可動する。その際、どのサイトマップか判別できるように処理中のサイトマップと連動するカウント。
    _sitemap_urls_count: int = 0
    # crawler_controllerコレクションへ書き込むレコードのdomain以降のレイアウト雛形。※最上位のKeyのdomainはサイトの特性にかかわらず固定とするため。
    _sitemap_contened: dict = {name: {}, }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        def generate_sitemap_urls():
            #'https://www.yomiuri.co.jp/sitemap-pt-post-2021-05-04.xml',
            #'https://www.yomiuri.co.jp/sitemap-pt-post-2021-05-03.xml',
            _now = datetime.now().astimezone(timezone(timedelta(hours=9), 'JST'))

            u = 'https://www.yomiuri.co.jp/sitemap-pt-post-%s.xml' % (_now.strftime('%Y-%m-%d'))
            self.sitemap_urls = [u]
            print(self.sitemap_urls)

        generate_sitemap_urls()
        print('=== YomiuriCoJpSitemapSpider の__init__終了')

    def sitemap_filter(self, entries):
        '''
        親クラスのSitemapSpiderの同名メソッドをオーバーライド。
        entriesには、サイトマップから取得した情報(loc,lastmodなど）が辞書形式で格納されている。
        例)entryの使い方：entry['lastmod'],entry['loc'],entry.items()
        サイトマップから取得したurlへrequestを行う前に条件で除外することができる。
        対象のurlの場合、”yield entry”で返すと親クラス側でrequestされる。
        '''
        # ExtensionsSitemapSpiderクラスを継承した場合のsitemap_filter共通処理
        self.sitemap_filter_common_prosses(entries)

        # 処理中のサイトマップ内で、最大のlastmodとurlを記録するエリア
        _max_lstmod: str = ''
        _max_url: str = ''

        for _entry in entries:
            '''
            引数に絞り込み指定がある場合、その条件を満たす場合のみ対象とする。
            '''
            if _max_lstmod < _entry['lastmod']:
                _max_lstmod = _entry['lastmod']
                _max_url = _entry['loc']

            _crwal_flg: bool = True
            date_lastmod = parser.parse(_entry['lastmod']).astimezone(
                self.settings['TIMEZONE'])
            if self._url_pattern != '':                    # url絞り込み指定あり
                _pattern = re.compile(self._url_pattern)
                if _pattern.search(_entry['loc']) == None:
                    _crwal_flg = False
            if self._url_term_days != 0:                       # 期間指定あり
                _pattern = re.compile('|'.join(self._url_tarm_days_list_yyyy_mm_dd))
                if _pattern.search(_entry['loc']) == None:
                    _crwal_flg = False
            if self._lastmod_recent_time != 0:             # lastmod絞り込み指定あり
                if date_lastmod < self._until_this_time:
                    _crwal_flg = False
            if self._sitemap_continued:                            # 前回クロールからの続きの場合
                if date_lastmod < self._until_this_time:
                    _crwal_flg = False
                elif date_lastmod == self._until_this_time \
                        and self._crawler_controller_recode[self.name][self.sitemap_urls[self._sitemap_urls_count]]['latest_url']:
                    _crwal_flg = False

            if _crwal_flg:
                yield _entry

        # crawler_controllerコレクションへ保存する内容を設定
        self._sitemap_contened[self.name][self.sitemap_urls[self._sitemap_urls_count]] = {
            'latest_lastmod': _max_lstmod,
            'latest_url': _max_url,
            'crawl_start_time': self._crawl_start_time_iso,
        }

        self._sitemap_urls_count += 1  # 次のサイトマップurl用にカウントアップ
