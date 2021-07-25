import sys
from news_crawl.spiders.extensions_class.extensions_sitemap import ExtensionsSitemapSpider
from news_crawl.spiders.common.term_days_Calculation import term_days_Calculation


class KyodoCoJpSitemapSpider(ExtensionsSitemapSpider):
    name = 'kyodo_co_jp_sitemap'
    allowed_domains = ['kyodo.co.jp']
    start_urls = ['http://kyodo.co.jp/']
    sitemap_urls: list = []
    _domain_name: str = 'kyodo_co_jp'        # 各種処理で使用するドメイン名の一元管理
    _spider_version: float = 1.0

    def __init__(self, *args, **kwargs):
        ''' (拡張メソッド)
        親クラスの__init__処理後に追加で初期処理を行う。
        '''
        super().__init__(*args, **kwargs)

        # 単項目チェック（追加）
        if not 'sitemap_term_days' in kwargs:
            sys.exit('引数エラー：当スパイダー(' + self.name +
                     ')の場合、sitemap_term_daysは必須です。')

        # 以下のようなurlを生成する。
        #     'https://www.kyodo.co.jp/sitemap-pt-post-2021-05.xml',
        #     'https://www.kyodo.co.jp/sitemap-pt-post-2021-04.xml',

        _sitemap_term_days_list = term_days_Calculation(
            self._crawl_start_time, int(self.kwargs_save['sitemap_term_days']), '%Y-%m')

        self.sitemap_urls = [
            'https://www.kyodo.co.jp/sitemap-pt-post-%s.xml' % (i) for i in _sitemap_term_days_list]
        # ※重複した同月のURLは自動的にScrapyによって除去される。

        self.logger.info('=== __init__ sitemap_urls 生成完了: %s',
                         self.sitemap_urls)
