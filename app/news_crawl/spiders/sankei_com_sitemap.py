from news_crawl.spiders.extensions_class.extensions_sitemap import ExtensionsSitemapSpider
from scrapy.spiders import Rule
from scrapy.linkextractors import LinkExtractor


class SankeiComSitemapSpider(ExtensionsSitemapSpider):
    name: str = 'sankei_com_sitemap'
    allowed_domains: list = ['sankei.com']
    sitemap_urls: list = [
        'https://www.sankei.com/robots.txt',
        #'https://feed.etf.sankei.com/global/sitemap',
        #'https://www.sankei.com/feeds/google-sitemapindex/?outputType=xml',
    ]
    _domain_name: str = 'sankei_com'        # 各種処理で使用するドメイン名の一元管理
    _spider_version: float = 1.0

    sitemap_follow = [
        '/feeds/google-sitemapindex/',
        '/feeds/google-sitemap/',
        ]

    #splash_mode: bool = True
    selenium_mode: bool = True
    sitemap_rules = [(r'.*', 'selenium_parse')]
    custom_settings = {
        'DOWNLOADER_MIDDLEWARES' : {
            'news_crawl.scrapy_selenium_custom_middlewares.SeleniumMiddleware': 585,
        }
    }

    sitemap_type = ExtensionsSitemapSpider.SITEMAP_TYPE__GOOGLE_NEWS_SITEMAP    #googleのニュースサイトマップ用にカスタマイズしたタイプ

    known_pagination_css_selectors:list[str] = [
        '.pagination  a[href]',
    ]

