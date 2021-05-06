from dateutil import parser
import re
from news_crawl.spiders.extensions_sitemap import ExtensionsSitemapSpider
from datetime import datetime, timedelta


class SankeiComSitemapSpider(ExtensionsSitemapSpider):
    name: str = 'sankei_com_sitemap'
    allowed_domains: list = ['sankei.com']
    sitemap_urls: list = ['https://www.sankei.com/sitemap.xml', ]
    _domain_name: str = 'sankei_com'        # 各種処理で使用するドメイン名の一元管理
    spider_version: float = 1.0

    # sitemap_urlsに複数のサイトマップを指定した場合、その数だけsitemap_filterが可動する。その際、どのサイトマップか判別できるように処理中のサイトマップと連動するカウント。
    _sitemap_urls_count: int = 0
    # crawler_controllerコレクションへ書き込むレコードのdomain以降のレイアウト雛形。※最上位のKeyのdomainはサイトの特性にかかわらず固定とするため。
    _sitemap_next_crawl_info: dict = {name: {}, }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        print('=== SankeiComSitemapSpider の__init__終了')

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

        # 直近の数分間の指定がある場合
        until_this_time: datetime = self._crawl_start_time
        if 'lastmod_recent_time' in self.kwargs_save:
            until_this_time = until_this_time - \
                timedelta(minutes=int(self.kwargs_save['lastmod_recent_time']))

        # urlに含まれる日付に指定がある場合
        _url_term_days_list: list = []
        if 'url_term_days' in self.kwargs_save:   #
            _url_term_days_list = self.term_days_Calculation(
                self._crawl_start_time, int(self.kwargs_save['url_term_days']), '%y%m%d')

        # 前回からの続きの指定がある場合
        _last_time:datetime = datetime.now()
        if 'sitemap_continued' in self.kwargs_save:
            _last_time = parser.parse(
                self._crawler_controller_recode[self.name][self.sitemap_urls[self._sitemap_urls_count]]['latest_lastmod'])

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
            _date_lastmod = parser.parse(_entry['lastmod']).astimezone(
                self.settings['TIMEZONE'])

            if 'url_pattern' in self.kwargs_save:   # url絞り込み指定あり
                pattern = re.compile(self.kwargs_save['url_pattern'])
                if pattern.search(_entry['loc']) == None:
                    _crwal_flg = False
            if 'url_term_days' in self.kwargs_save:                       # 期間指定あり
                _pattern = re.compile('|'.join(_url_term_days_list))
                if _pattern.search(_entry['loc']) == None:
                    _crwal_flg = False
            if 'lastmod_recent_time' in self.kwargs_save:             # lastmod絞り込み指定あり
                if _date_lastmod < until_this_time:
                    _crwal_flg = False
            if 'sitemap_continued' in self.kwargs_save:
                if _date_lastmod < _last_time:
                    _crwal_flg = False
                elif _date_lastmod == _last_time \
                        and self._crawler_controller_recode[self.name][self.sitemap_urls[self._sitemap_urls_count]]['latest_url']:
                    _crwal_flg = False

            if _crwal_flg:
                yield _entry

        # サイトマップごとの最大更新時間を記録(crawler_controllerコレクションへ保存する内容)
        self._sitemap_next_crawl_info[self.name][self.sitemap_urls[self._sitemap_urls_count]] = {
            'latest_lastmod': _max_lstmod,
            'latest_url': _max_url,
            'crawl_start_time': self._crawl_start_time_iso,
        }

        self._sitemap_urls_count += 1  # 次のサイトマップurl用にカウントアップ
