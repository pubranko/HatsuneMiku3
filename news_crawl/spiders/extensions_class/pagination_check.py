from __future__ import annotations
import pickle
import scrapy
import re
from typing import Union, Any
from datetime import datetime
from dateutil import parser
from lxml.etree import _Element

from urllib.parse import urlparse, urljoin, parse_qs, unquote
from urllib.parse import ParseResult

from scrapy.spiders import SitemapSpider
from scrapy.spiders.sitemap import iterloc
from scrapy.http import Response, Request, TextResponse
from scrapy.utils.sitemap import sitemap_urls_from_robots
from scrapy_selenium import SeleniumRequest
from scrapy_splash import SplashRequest
from scrapy_splash.response import SplashTextResponse
from selenium.webdriver.remote.webdriver import WebDriver
from news_crawl.items import NewsCrawlItem
from models.mongo_model import MongoModel
from news_crawl.spiders.common.start_request_debug_file_generate import start_request_debug_file_generate
from news_crawl.spiders.common.spider_init import spider_init
from news_crawl.spiders.common.spider_closed import spider_closed
from news_crawl.spiders.common.lastmod_period_skip_check import LastmodPeriodMinutesSkipCheck
from news_crawl.spiders.common.lastmod_continued_skip_check import LastmodContinuedSkipCheck
from news_crawl.spiders.common.url_pattern_skip_check import url_pattern_skip_check
from news_crawl.spiders.common.custom_sitemap import CustomSitemap


class PaginationCheck():
    '''
    '''
    def pagination_check(self, response: TextResponse, link_url: str) -> bool:
        ''' '''
        check_flg: bool = False  # ページネーションのリンクの場合、Trueとする。
        # チェック対象のurlを解析
        link_parse: ParseResult = urlparse(link_url)
        # 解析したクエリーをdictへ変換 page=2&a=1&b=2 -> {'page': ['2'], 'a': ['1'], 'b': ['2']}
        link_query: dict = parse_qs(link_parse.query)

        pagination_selected_parses:list = []
        pagination_selected_pathes:list = []
        pagination_selected_queries:list = []
        for pagination_selected_url in self.pagination_selected_urls:
            _ = urlparse(pagination_selected_url)
            pagination_selected_pathes.append(_.path)
            pagination_selected_queries.append(parse_qs(_.query))



        # sitemapから取得したurlより順にチェック
        for _ in self.crawl_target_urls:
            # sitemapから取得したurlを解析
            crawl_target_parse: ParseResult = urlparse(_)

            # netloc（hostnameだけでなくportも含む）が一致すること
            if crawl_target_parse.netloc == link_parse.netloc:

                # まだ同一ページの追加リクエストされていない場合（path部分で判定）
                if not link_parse.path in pagination_selected_pathes:
                    # パスの末尾にページが付与されているケースの場合、追加リクエストの対象とする。
                    # 例）https://www.sankei.com/article/20210321-VW5B7JJG7JKCBG5J6REEW6ZTBM/
                    #     https://www.sankei.com/article/20210321-VW5B7JJG7JKCBG5J6REEW6ZTBM/2/
                    _ = re.compile(r'/[0-9]{1,3}/*$')
                    if re.search(_, link_parse.path):
                        link_type1 = _.sub('/', link_parse.path)
                        # 例）/world/20220430-OYT1T50226/   -> /world/20220430-OYT1T50226
                        _ = re.compile(r'/$')
                        crawl_type1 = _.sub('/', crawl_target_parse.path)

                        if crawl_type1 == link_type1:
                            self.logger.info(
                                f'=== {self.name} ページネーションtype1 : {link_url}')
                            check_flg = True

                    # 拡張子除去後の末尾にページが付与されているケースの場合、追加リクエストの対象とする。
                    # 例）https://www.sankei.com/politics/news/210521/plt2105210030-n1.html
                    #     https://www.sankei.com/politics/news/210521/plt2105210030-n2.html
                    _ = re.compile(r'[^0-9][0-9]{1,3}.[html|htm]$')
                    if re.search(_, link_parse.path):
                        link_type2 = _.sub('/', link_parse.path)
                        # 例）politics/news/210521/plt2105210030-n1.html -> politics/news/210521/plt2105210030-n
                        _ = re.compile(r'[^0-9][0-9]{1,3}.[html|htm]$')
                        crawl_type2 = _.sub('/', crawl_target_parse.path)

                        if crawl_type2 == link_type2:
                            self.logger.info(
                                f'=== {self.name} ページネーションtype2 : {link_url}')
                            check_flg = True

                # クエリーにページが付与されているケースの場合、追加リクエストの対象とする。
                # ただし、以下の場合は対象外。
                # ・既に同一ページの追加リクエスト済みの場合。
                # ・１ページ目の場合。※sitemap側でリクエスト済みのため。
                # 例）https://webronza.asahi.com/national/articles/2022042000004.html
                #     https://webronza.asahi.com/national/articles/2022042000004.html?a=b&c=d
                #     https://webronza.asahi.com/national/articles/2022042000004.html?page=1&a=b&e=f
                #     https://webronza.asahi.com/national/articles/2022042000004.html?page=1&m=n&g=h
                #     https://webronza.asahi.com/national/articles/2022042000004.html?page=2&a=b&e=f
                #     https://webronza.asahi.com/national/articles/2022042000004.html?page=2&m=n&g=h
                if crawl_target_parse.path == link_parse.path:
                    # リンクのクエリーにページ指定と思われるkeyの存在チェック （複数該当することは無いことを祈る、、、）
                    page_keys = ['page', 'pagination', 'pager', 'p']
                    #query_selected_items = [(query_key,query_value) if  query_key in page_keys else None for query_key,query_value in link_query.items()]
                    link_query_selected_items:list[tuple] = []
                    for link_query_key,link_query_value in link_query.items():
                        if  link_query_key in page_keys:
                            link_query_selected_items.append((link_query_key,link_query_value))

                    # linkにpege系クエリーがあった場合、
                    for link_query_selected_item in link_query_selected_items:
                        check_flg = True
                        for pagination_selected_url in self.pagination_selected_urls:
                            _ = urlparse(pagination_selected_url)
                            pagination_selected_query: dict = parse_qs(_.query)
                            if link_query_selected_item[0] in pagination_selected_query: #keyが一致
                                if link_query_selected_item[1][0] == pagination_selected_query[link_query_selected_item[0]][0]: #valueが一致(同一ページ)した場合は対象外
                                    check_flg = False
                                elif link_query_selected_item[1][0] == str(1):   #page=1は対象外
                                    check_flg = False
                        if check_flg:
                            self.logger.info(
                                f'=== {self.name} ページネーションtype3 : {link_url}')

        # クロール対象となったurlを保存
        if check_flg:
            self.pagination_selected_urls.add(link_url)

        return check_flg
