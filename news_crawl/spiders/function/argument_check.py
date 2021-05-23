import sys
import os
import re
from typing import Any
from scrapy.spiders import Spider


def argument_check(spider: Spider, domain_name: str, crawler_controller_recode: Any, *args, **kwargs) -> None:
    '''
    各引数が存在したらチェックを行う。
    '''
    # 単項目チェック

    def __debug_check() -> None:
        if kwargs['debug'] == 'Yes':
            spider.logger.info('=== debugモード ON: %s', spider.name)
            # デバック用のファイルを初期化
            path = os.path.join(
                'debug', 'start_urls(' + str(spider.name) + ').txt')
            with open(path, 'w') as _:
                pass
        else:
            sys.exit('引数エラー：debugに指定できるのは"Yes"のみです。')

    def __url_term_days_check() -> None:
        if not kwargs['url_term_days'].isdecimal():
            sys.exit('引数エラー：url_term_daysは数字のみ使用可。日単位で指定してください。')
        elif kwargs['url_term_days'] == 0:
            sys.exit('引数エラー：url_term_daysは0日の指定は不可です。1日以上を指定してください。')

    def __sitemap_term_days_check() -> None:
        if not kwargs['sitemap_term_days'].isdecimal():
            sys.exit('引数エラー：sitemap_term_daysは数字のみ使用可。日単位で指定してください。')
        elif kwargs['sitemap_term_days'] == 0:
            sys.exit('引数エラー：sitemap_term_daysは0日の指定は不可です。1日以上を指定してください。')

    def __lastmod_recent_time_check() -> None:
        if not kwargs['lastmod_recent_time'].isdecimal():
            sys.exit('引数エラー：lastmod_recent_timeは数字のみ使用可。分単位で指定してください。')
        elif kwargs['lastmod_recent_time'] == 0:
            sys.exit('引数エラー：lastmod_recent_timeは0分の指定は不可です。')

    def __continued_check() -> None:
        if kwargs['continued'] == 'Yes':
            if crawler_controller_recode == None:
                sys.exit('引数エラー：domain = ' + domain_name +
                         ' は前回のcrawl情報がありません。初回から"continued"の使用は不可です。')
        else:
            sys.exit('引数エラー：continuedに使用できるのは、"Yes"のみです。')

    def __pages_check() -> None:
        if 'pages' in kwargs:
            ptn = re.compile(r'^\[[0-9]+,[0-9]+\]$$')
            if ptn.search(kwargs['pages']):
                pages = eval(kwargs['pages'])
                if pages[0] > pages[1]:
                    sys.exit(
                        '引数エラー：pagesの開始ページと終了ページは開始≦終了で指定してください。（エラー例）[3,2] （値 = ' + kwargs['pages']+')')
            else:
                sys.exit(
                    '引数エラー：pagesは配列形式[num,num]で開始・終了ページを指定してください。（例）[2,3] （値 = ' + kwargs['pages']+')')

    # 項目関連チェック

    def __lastmod_recent_time_and_continued() -> None:
        sys.exit('引数エラー：lastmod_recent_timeとcontinuedは同時には使えません。')

    ### 単項目チェック ###
    if 'debug' in kwargs:
        __debug_check()
    if 'url_term_days' in kwargs:
        __url_term_days_check()
    if 'sitemap_term_days' in kwargs:
        __sitemap_term_days_check()
    if 'lastmod_recent_time' in kwargs:
        __lastmod_recent_time_check()
    if 'continued' in kwargs:
        __continued_check()
    if 'pages' in kwargs:
        __pages_check()

    ### 項目関連チェック ###
    if 'lastmod_recent_time' in kwargs and 'continued' in kwargs:
        __lastmod_recent_time_and_continued()
