import sys
from news_crawl.spiders.extensions_sitemap import ExtensionsSitemapSpider
from datetime import timedelta


class JpReutersComSitemapSpider(ExtensionsSitemapSpider):
    name:str = 'jp_reuters_com_sitemap'
    allowed_domains = ['jp.reuters.com']
    sitemap_urls: list = []
    _domain_name: str = 'jp_reuters_com_crawl'        # 各種処理で使用するドメイン名の一元管理
    spider_version: float = 1.0

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
        #     https://jp.reuters.com/sitemap_20210505-20210506.xml,
        #     https://jp.reuters.com/sitemap_20210504-20210505.xml

        _sitemap_term_days_list_start = self.term_days_Calculation(
            self._crawl_start_time - timedelta(days=1), int(self.kwargs_save['sitemap_term_days']), '%Y%m%d')
        _sitemap_term_days_list_end = self.term_days_Calculation(
            self._crawl_start_time, int(self.kwargs_save['sitemap_term_days']), '%Y%m%d')

        self.sitemap_urls = [
            #'https://jp.reuters.com/sitemap_index.xml']
            'https://jp.reuters.com/sitemap_%s-%s.xml' % (s, e) for s, e in zip(_sitemap_term_days_list_start, _sitemap_term_days_list_end)]

        self.logger.info('=== __init__ sitemap_urls 生成完了: %s',
                         self.sitemap_urls)
