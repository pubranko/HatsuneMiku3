from news_crawl.spiders.extensions_sitemap import ExtensionsSitemapSpider
from datetime import datetime
import re
import os
from datetime import datetime, timedelta
from dateutil import parser
from scrapy.utils.sitemap import Sitemap


class AsahiComSitemapSpider(ExtensionsSitemapSpider):
    name = 'asahi_com_sitemap'
    allowed_domains = ['asahi.com']
    sitemap_urls: list = [
        #'http://www.asahi.com/sitemap.xml',
        'https://www.asahi.com/sitemap/sitemap_national.xml',
        'https://www.asahi.com/sitemap/sitemap_business.xml',
        'https://www.asahi.com/sitemap/sitemap_politics.xml',
        'https://www.asahi.com/sitemap/sitemap_sports.xml',
        'https://www.asahi.com/sitemap/sitemap_international.xml',
        'https://www.asahi.com/sitemap/sitemap_culture.xml',
        'https://www.asahi.com/sitemap/sitemap_science.xml',
        'https://www.asahi.com/sitemap/sitemap_obituaries.xml',
    ]
    _domain_name: str = 'asahi_com'        # 各種処理で使用するドメイン名の一元管理
    _spider_version: float = 1.0

    # sitemap_urlsに複数のサイトマップを指定した場合、その数だけsitemap_filterが可動する。その際、どのサイトマップか判別できるように処理中のサイトマップと連動するカウント。
    _sitemap_urls_count: int = 0
    # crawler_controllerコレクションへ書き込むレコードのdomain以降のレイアウト雛形。※最上位のKeyのdomainはサイトの特性にかかわらず固定とするため。
    _sitemap_next_crawl_info: dict = {name: {}, }

    def sitemap_filter(self, entries: Sitemap):
        '''
        親クラスのSitemapSpiderの同名メソッドをオーバーライド。
        entriesには、サイトマップから取得した情報(loc,lastmodなど）が辞書形式で格納されている。
        例)entryの使い方：entry['lastmod'],entry['loc'],entry.items()
        サイトマップから取得したurlへrequestを行う前に条件で除外することができる。
        対象のurlの場合、”yield entry”で返すと親クラス側でrequestされる。
        '''
        # ExtensionsSitemapSpiderクラスを継承した場合のsitemap_filter共通処理
        print()
        self.sitemap_entries_debug_file_generate(
            entries, self.sitemap_urls[self._sitemap_urls_count])

        # 直近の数分間の指定がある場合
        until_this_time: datetime = self._crawl_start_time
        if 'lastmod_recent_time' in self.kwargs_save:
            until_this_time = until_this_time - \
                timedelta(minutes=int(self.kwargs_save['lastmod_recent_time']))
            self.logger.info(
                '=== sitemap_filter : lastmod_recent_timeより計算した時間 %s', until_this_time.isoformat())

        # urlに含まれる日付に指定がある場合
        _url_term_days_list: list = []
        if 'url_term_days' in self.kwargs_save:   #
            _url_term_days_list = self.term_days_Calculation(
                self._crawl_start_time, int(self.kwargs_save['url_term_days']), '%y%m%d')
            self.logger.info(
                '=== sitemap_filter : url_term_daysより計算した日付 %s', ', '.join(_url_term_days_list))

        # 前回からの続きの指定がある場合
        _last_time: datetime = datetime.now()
        if 'continued' in self.kwargs_save:
            _last_time = parser.parse(
                self._crawler_controller_recode[self.name][self.sitemap_urls[self._sitemap_urls_count]]['latest_lastmod'])

        # 処理中のサイトマップ内で、最大のlastmodとurlを記録するエリア
        _max_lstmod: str = ''
        _max_url: str = ''

        for _entry in entries:
            '''
            引数に絞り込み指定がある場合、その条件を満たす場合のみ対象とする。
            '''
            _entry:dict

            if _max_lstmod < _entry['news:publication_date']:
                _max_lstmod = _entry['news:publication_date']
                _max_url = _entry['loc']

            _crwal_flg: bool = True
            _date_lastmod = parser.parse(_entry['news:publication_date']).astimezone(
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
            if 'continued' in self.kwargs_save:
                if _date_lastmod < _last_time:
                    _crwal_flg = False
                elif _date_lastmod == _last_time \
                        and self._crawler_controller_recode[self.name][self.sitemap_urls[self._sitemap_urls_count]]['latest_url']:
                    _crwal_flg = False

            if _crwal_flg:
                if self._custom_url_flg:
                    _entry['loc'] = self._custom_url(_entry)

                yield _entry

        # サイトマップごとの最大更新時間を記録(crawler_controllerコレクションへ保存する内容)
        self._sitemap_next_crawl_info[self.name][self.sitemap_urls[self._sitemap_urls_count]] = {
            'latest_lastmod': _max_lstmod,
            'latest_url': _max_url,
            'crawl_start_time': self._crawl_start_time_iso,
        }

        self._sitemap_urls_count += 1  # 次のサイトマップurl用にカウントアップ

    def sitemap_entries_debug_file_generate(self, entries, sitemap_url: str):
        ''' (拡張メソッド)
        デバックモードが指定された場合、entriesと元となったsitemapのurlをdebug_entries.txtへ出力する。
        '''
        if 'debug' in self.kwargs_save:         # sitemap調査用。debugモードの場合のみ。
            path: str = os.path.join(
                'debug', 'start_urls(' + self.name + ').txt')
            with open(path, 'a') as _:
                for _entry in entries:
                    print('=== deubg : ',_entry)
                    _.write(sitemap_url + ',' +
                            _entry['news:publication_date'] + ',' + _entry['loc'] + '\n')

