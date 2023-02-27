import sys
from scrapy.exceptions import CloseSpider
from news_crawl.spiders.extensions_class.extensions_sitemap import ExtensionsSitemapSpider
from datetime import timedelta
from news_crawl.spiders.common.term_days_Calculation import term_days_Calculation

'''
このソースは現在未使用。
'''

class JpReutersComSitemapSpider(ExtensionsSitemapSpider):
    name: str = 'jp_reuters_com_sitemap'
    allowed_domains = ['jp.reuters.com']
    sitemap_urls: list = []
    _domain_name: str = 'jp_reuters_com'        # 各種処理で使用するドメイン名の一元管理
    _spider_version: float = 1.0

    # 廃止された項目だがエラーが出ないようにとりあえず定義しているだけ。
    kwargs_save:dict

    def __init__(self, *args, **kwargs):
        ''' (拡張メソッド)
        親クラスの__init__処理後に追加で初期処理を行う。
        '''
        super().__init__(*args, **kwargs)

        # 単項目チェック（追加）
        if not 'sitemap_term_days' in kwargs:
            raise CloseSpider('引数エラー：当スパイダー(' + self.name +
                     ')の場合、sitemap_term_daysは必須です。')

        # 以下のようなurlを生成する。
        #     https://jp.reuters.com/sitemap_20210505-20210506.xml,
        #     https://jp.reuters.com/sitemap_20210504-20210505.xml
        _sitemap_term_days_list_start = term_days_Calculation(
            self.news_crawl_input.crawling_start_time - timedelta(days=1), int(self.kwargs_save['sitemap_term_days']), '%Y%m%d')
        _sitemap_term_days_list_end = term_days_Calculation(
            self.news_crawl_input.crawling_start_time, int(self.kwargs_save['sitemap_term_days']), '%Y%m%d')

        self.sitemap_urls = [
            'https://jp.reuters.com/sitemap_%s-%s.xml' % (s, e) for s, e in zip(_sitemap_term_days_list_start, _sitemap_term_days_list_end)]

        self.logger.info(f'=== __init__ sitemap_urls 生成完了: {self.sitemap_urls}')
