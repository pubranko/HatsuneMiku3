import sys
from news_crawl.spiders.extensions_sitemap import ExtensionsSitemapSpider
from news_crawl.spiders.function.term_days_Calculation import term_days_Calculation

class YomiuriCoJpSitemapSpider(ExtensionsSitemapSpider):
    name: str = 'yomiuri_co_jp_sitemap'
    allowed_domains: list = ['yomiuri.co.jp']
    sitemap_urls: list = []
    _domain_name: str = 'yomiuri_co_jp'        # 各種処理で使用するドメイン名の一元管理
    _spider_version: float = 1.0

    # sitemap_urlsに複数のサイトマップを指定した場合、その数だけsitemap_filterが可動する。その際、どのサイトマップか判別できるように処理中のサイトマップと連動するカウント。
    _sitemap_urls_count: int = 0
    # crawler_controllerコレクションへ書き込むレコードのdomain以降のレイアウト雛形。※最上位のKeyのdomainはサイトの特性にかかわらず固定とするため。
    _sitemap_next_crawl_info: dict = {name: {}, }

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
        #     'https://www.yomiuri.co.jp/sitemap-pt-post-2021-05-04.xml',
        #     'https://www.yomiuri.co.jp/sitemap-pt-post-2021-05-03.xml',

        _sitemap_term_days_list = term_days_Calculation(
            self._crawl_start_time, int(self.kwargs_save['sitemap_term_days']), '%Y-%m-%d')
        self.sitemap_urls = [
            'https://www.yomiuri.co.jp/sitemap-pt-post-%s.xml' % (i) for i in _sitemap_term_days_list]

        self.logger.info('=== __init__ sitemap_urls 生成完了: %s',
                         self.sitemap_urls)
