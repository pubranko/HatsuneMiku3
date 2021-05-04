from datetime import datetime, timedelta
from dateutil import parser
import re
from news_crawl.spiders.extensions_sitemap import ExtensionsSitemapSpider

class ExtensionsSitemapFilterTyep1Spider(ExtensionsSitemapSpider):
    '''
    SitemapSpiderの機能を拡張したクラス。
    Override → sitemap_filter()
    (前提)name,allowed_domains,sitemap_urls,_domain_nameの値は当クラスを継承するクラスで設定すること
    '''
    name = 'extension_sitemap_filter_type1'

    def sitemap_filter(self, entries):
        '''
        親クラスのSitemapSpiderの同名メソッドをオーバーライド。
        entriesには、サイトマップから取得した情報(loc,lastmodなど）が辞書形式で格納されている。
        例)entryの使い方：entry['lastmod'],entry['loc'],entry.items()
        サイトマップから取得したurlへrequestを行う前に条件で除外することができる。
        対象のurlの場合、”yield entry”で返すと親クラス側でrequestされる。
        '''
        # sitemap_filter共通処理
        self.sitemap_filter_common_prosses(entries)

        # 1件目のlastmod（最新の更新）とurlを取得
        _entry: dict = next(iter(entries))
        self._latest_lastmod = _entry['lastmod']
        self._latest_url = _entry['loc']

        for _entry in entries:
            '''
            引数に絞り込み指定がある場合、その条件を満たす場合のみ対象とする。
            '''
            _crwal_flg: bool = True
            date_lastmod = parser.parse(_entry['lastmod']).astimezone(
                self.settings['TIMEZONE'])
            if self._url_pattern != '':                    # url絞り込み指定あり
                _pattern = re.compile(self._url_pattern)
                if _pattern.search(_entry['loc']) == None:
                    _crwal_flg = False
            if self._term_days != 0:                       # 期間指定あり
                _pattern = re.compile('|'.join(self._tarm_days_list))
                if _pattern.search(_entry['loc']) == None:
                    _crwal_flg = False
            if self._lastmod_recent_time != 0:             # lastmod絞り込み指定あり
                if date_lastmod < self._until_this_time:
                    _crwal_flg = False
            if self._continued:                            # 前回クロールからの続きの場合
                if date_lastmod < self._until_this_time:
                    _crwal_flg = False
                elif date_lastmod == self._until_this_time \
                        and self._crawler_controller_recode['spider_name'][self.name]['latest_url']:
                    _crwal_flg = False

            if _crwal_flg:
                yield _entry
            # else:
            #     _pattern = re.compile('.xml$')
            #     if _pattern.search(_entry['loc']):
            #         #print('.')
            #         yield _entry

