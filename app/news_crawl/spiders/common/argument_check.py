from __future__ import annotations  # ExtensionsSitemapSpiderの循環参照を回避するため
import re
import copy
from typing import Union, Any, TYPE_CHECKING
from scrapy.exceptions import CloseSpider
from datetime import datetime
from urllib.parse import urlparse

if TYPE_CHECKING:  # 型チェック時のみインポート
    from news_crawl.spiders.extensions_class.extensions_sitemap import ExtensionsSitemapSpider
    from news_crawl.spiders.extensions_class.extensions_crawl import ExtensionsCrawlSpider
    from news_crawl.spiders.extensions_class.extensions_xml_feed import ExtensionsXmlFeedSpider


def argument_check(
    spider: Union[ExtensionsSitemapSpider, ExtensionsCrawlSpider, ExtensionsXmlFeedSpider],
    domain_name: str,
    controller_recode: dict,
    *args,
    **kwargs
) -> None:
    '''
    各引数が存在したらチェックを行う。
    '''
    # 単項目チェック
    def crawling_start_time() -> None:
        if not isinstance(kwargs['crawling_start_time'], datetime):
            spider.logger.critical(
                '引数エラー：crawling_start_timeにはdatetimeオブジェクトのみ設定可能。')
            raise CloseSpider()

    def debug_check() -> None:
        if not kwargs['debug'] == 'Yes':
            spider.logger.critical('引数エラー：debugに指定できるのは"Yes"のみです。')
            raise CloseSpider()

    def url_term_days_check() -> None:
        if not kwargs['url_term_days'].isdecimal():
            spider.logger.critical('引数エラー：url_term_daysは数字のみ使用可。日単位で指定してください。')
            raise CloseSpider()
        elif kwargs['url_term_days'] == 0:
            spider.logger.critical(
                '引数エラー：url_term_daysは0日の指定は不可です。1日以上を指定してください。')
            raise CloseSpider()

    def sitemap_term_days_check() -> None:
        if not kwargs['sitemap_term_days'].isdecimal():
            spider.logger.critical(
                '引数エラー：sitemap_term_daysは数字のみ使用可。日単位で指定してください。')
            raise CloseSpider()
        elif kwargs['sitemap_term_days'] == 0:
            spider.logger.critical(
                '引数エラー：sitemap_term_daysは0日の指定は不可です。1日以上を指定してください。')
            raise CloseSpider()

    def lastmod_recent_time_check() -> None:
        if not kwargs['lastmod_recent_time'].isdecimal():
            spider.logger.critical(
                '引数エラー：lastmod_recent_timeは数字のみ使用可。分単位で指定してください。')
            raise CloseSpider()
        elif kwargs['lastmod_recent_time'] == 0:
            spider.logger.critical('引数エラー：lastmod_recent_timeは0分の指定は不可です。')
            raise CloseSpider()

    def continued_check() -> None:
        if kwargs['continued'] == 'Yes':
            if controller_recode == {}:
                spider.logger.critical(
                    '引数エラー：domain = ' + domain_name + ' は前回のcrawl情報がありません。初回から"continued"の使用は不可です。')
                raise CloseSpider()
        else:
            spider.logger.critical('')
            raise CloseSpider('引数エラー：continuedに使用できるのは、"Yes"のみです。')

    def pages_check() -> None:
        #ptn = re.compile(r'^\[[0-9]+,[0-9]+\]$')
        ptn = re.compile(r'^[0-9]+,[0-9]+$')
        if ptn.search(kwargs['pages']):
            pages = kwargs['pages'].split(',')
            if pages[0] > pages[1]:
                spider.logger.critical(
                    '引数エラー：pagesの開始ページと終了ページは開始≦終了で指定してください。（エラー例）3,2 （値 = ' + kwargs['pages'] + '）')
                raise CloseSpider()
        else:
            spider.logger.critical(
                '引数エラー：pagesはカンマ区切りで開始・終了ページを指定してください。（例）2,3 （値 = ' + kwargs['pages'] + '）')
            raise CloseSpider()

    def category_urls() -> None:
        ptn = re.compile(r'^\[.+\]$')
        if not ptn.search(kwargs['category_urls']):
            spider.logger.critical(
                '引数エラー：category_urlsは配列形式[Any,,,]で指定してください。（例）[100,108] （値 = ' + kwargs['category_urls'] + '）')
            raise CloseSpider()

    def error_notice() -> None:
        if kwargs['error_notice'] == 'Off':
            pass
        else:
            spider.logger.critical('引数エラー：error_noticeに使用できるのは、"Off"のみです。')
            raise CloseSpider()

    def lastmod_period_minutes() -> None:
        ptn = re.compile(r'^[0-9]*,[0-9]*$')  # リスト形式かチェック
        if ptn.search(kwargs['lastmod_period_minutes']):
            lastmod_period_minutes = str(
                kwargs['lastmod_period_minutes']).split(',')

            if lastmod_period_minutes[0] == '' and \
               lastmod_period_minutes[1] == '':
                spider.logger.critical(
                    '引数エラー：lastmod_period_minutesは、開始時間と終了時間のどちらかは指定してください。エラー例[,] 値 = ' + kwargs['lastmod_period_minutes'])
                raise CloseSpider()
            elif lastmod_period_minutes[0] == '':
                pass
            elif lastmod_period_minutes[1] == '':
                pass
            elif int(lastmod_period_minutes[0]) <= int(lastmod_period_minutes[1]):
                spider.logger.critical(
                    '引数エラー：lastmod_period_minutesは、開始時間と終了時間を開始＞終了で指定してください。（エラー例）[2,3] （値 = ' + kwargs['lastmod_period_minutes'] + '）')
                raise CloseSpider()
        else:
            spider.logger.critical(
                '引数エラー：lastmod_period_minutesは配列形式[int|None,int|None]で指定してください。（例）[60,10] （値 = ' + kwargs['lastmod_period_minutes'] + '）')
            raise CloseSpider()

    def crawl_point_non_update() -> None:
        if kwargs['crawl_point_non_update'] == 'Yes':
            pass
        else:
            spider.logger.critical(
                '引数エラー：crawl_point_non_updateに使用できるのは、"Yes"のみです。')
            raise CloseSpider()

    def direct_crawl_urls() -> None:
        if type(kwargs['direct_crawl_urls']) == list:
            for url in kwargs['direct_crawl_urls']:
                parsed_url = urlparse(url)
                if not spider.allowed_domains[0] == parsed_url.netloc.replace('www.', ''):
                    spider.logger.warning(
                        '引数ワーニングー：direct_crawl_urlsとspiderのドメイン不一致 : ' + url)
                    #raise CloseSpider()
        elif type(kwargs['direct_crawl_urls']) == str:
            parsed_url = urlparse(kwargs['direct_crawl_urls'])
            if not spider.allowed_domains[0] == parsed_url.netloc.replace('www.', ''):
                spider.logger.warning(
                    '引数ワーニング：direct_crawl_urlsとspiderのドメイン不一致 : ' + kwargs['direct_crawl_urls'])
                #raise CloseSpider()
            else:
                spider.kwargs_save['direct_crawl_urls'] = [copy.deepcopy(spider.kwargs_save['direct_crawl_urls'])]
                # これ以降は、direct_crawl_urlsの型をlistへ統一して後続の処理を行わせる。
        else:
            spider.logger.critical(
                '引数エラー：direct_crawl_urlsにはlist型またはstr型を指定してください。')
            raise CloseSpider()

    def url_pattern() -> None:
        if type(kwargs['url_pattern']) == str:
            pass
        else:
            spider.logger.critical(
                '引数エラー：url_patternに使用できるのはstr型を指定してください。')
            raise CloseSpider()

    # 項目関連チェック
    def lastmod_recent_time_and_continued() -> None:
        spider.logger.critical(
            '引数エラー：lastmod_recent_timeとcontinuedは同時には使えません。')
        raise CloseSpider()

    ### (前処理) keyの値がNoneの値の場合、引数なしとして扱うためkwargs、spider.kwargs_saveより削除する。
    for key in spider.kwargs_save.keys():
        if spider.kwargs_save[key] == None:
            del kwargs[key]
    spider.kwargs_save = kwargs

    ### 単項目チェック ###
    if 'crawling_start_time' in kwargs:
        crawling_start_time()
    if 'debug' in kwargs:
        debug_check()
    if 'url_term_days' in kwargs:
        url_term_days_check()
    if 'sitemap_term_days' in kwargs:
        sitemap_term_days_check()
    if 'lastmod_recent_time' in kwargs:
        lastmod_recent_time_check()
    if 'continued' in kwargs:
        continued_check()
    if 'pages' in kwargs:
        pages_check()
    if 'category_urls' in kwargs:
        category_urls()
    if 'error_notice' in kwargs:
        error_notice()
    if 'lastmod_period_minutes' in kwargs:
        lastmod_period_minutes()
    if 'crawl_point_non_update' in kwargs:
        crawl_point_non_update()
    if 'direct_crawl_urls' in kwargs:
        direct_crawl_urls()
    if 'url_pattern' in kwargs:
        url_pattern()

    ### 項目関連チェック ###
    if 'lastmod_recent_time' in kwargs and 'continued' in kwargs:
        lastmod_recent_time_and_continued()