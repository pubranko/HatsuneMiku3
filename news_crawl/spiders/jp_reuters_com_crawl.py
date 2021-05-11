from news_crawl.spiders.extensions_crawl import ExtensionsCrawlSpider
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import Rule


class JpReutersComCrawlSpider(ExtensionsCrawlSpider):
    name:str = 'jp_reuters_com_crawl'
    allowed_domains:list = ['jp.reuters.com']
    start_urls:list = [
        'http://jp.reuters.com/',
        #'https://jp.reuters.com/theWire',
        #'https://jp.reuters.com/news/topNews',
        ]
    _domain_name: str = 'jp_reuters_com_crawl'        # 各種処理で使用するドメイン名の一元管理
    spider_version: float = 1.0

    # sitemap_urlsに複数のサイトマップを指定した場合、その数だけsitemap_filterが可動する。その際、どのサイトマップか判別できるように処理中のサイトマップと連動するカウント。
    _crawl_urls_count: int = 0
    # crawler_controllerコレクションへ書き込むレコードのdomain以降のレイアウト雛形。※最上位のKeyのdomainはサイトの特性にかかわらず固定とするため。
    _cwawl_next_info: dict = {name: {}, }

    rules = (
        Rule(LinkExtractor(
            allow=(r'/article/')), callback='parse_news'),
    )
