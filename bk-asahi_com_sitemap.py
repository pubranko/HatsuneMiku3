import scrapy
from news_crawl.spiders.extensions_sitemap_filter_type1 import ExtensionsSitemapFilterTyep1Spider


class AsahiComSitemapSpider(ExtensionsSitemapFilterTyep1Spider):
    name = 'temp_asahi_com_sitemap'
    allowed_domains = ['asahi.com']
    start_urls = ['http://asahi.com/']

    #sitemap_urls = ['http://www.asahi.com/sitemap.xml', ]
    #sitemap_urls = ['https://www.asahi.com/robots.txt', ]
    sitemap_urls = [
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

    custom_settings = {
        'DEPTH_LIMIT': 2,
        'DEPTH_STATS_VERBOSE': True
    }
