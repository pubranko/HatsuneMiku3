from typing import Any, Tuple
from news_crawl.models.crawler_controller_model import CrawlerControllerModel
from news_crawl.models.mongo_model import MongoModel
import sys


def extensions_sitemap_argument_check(self, mongo: MongoModel, domain_name: str, *args, **kwargs) -> Tuple[bool, int, int, str, int, Any, bool]:
    '''あとで
    '''
    debug_flg: bool = False                 # debugモードを引数で指定された場合True
    url_term_days: int = 0                  # 当日を含め、指定した日数を含むurlに限定
    sitemap_term_days: int = 0              # 当日を含め、指定した日数を含むsitemapに限定
    url_pattern: str = ''                   # 指定したパターンをurlに含むもので限定(正規表現)
    lastmod_recent_time: int = 0            # 直近の15分など。分単位で指定することにしよう。0は制限なしとする。
    crawler_controller_recode: Any = None   # crawler_controllerコレクションのレコード
    sitemap_continued: bool = False         # 前回の続きから(最後に取得したlastmodの日時)

    ### 単項目チェック ###
    if 'debug' in kwargs:
        if kwargs['debug'] == 'Yes':
            debug_flg = True
            self.logger.info('=== debugモード ON: %s', self.name)
            _ = open('debug_entries.txt', 'w')  # 初期化
            _.close()
    if 'url_term_days' in kwargs:
        if kwargs['url_term_days'].isdecimal():
            url_term_days = int(kwargs['url_term_days'])
        else:
            sys.exit('引数エラー：url_term_daysは数字(0〜9)のみ使用可。日単位で指定してください。')
    if 'sitemap_term_days' in kwargs:
        if kwargs['sitemap_term_days'].isdecimal():
            sitemap_term_days = int(kwargs['sitemap_term_days'])
        else:
            sys.exit('引数エラー：sitemap_term_daysは数字(0〜9)のみ使用可。日単位で指定してください。')
    if 'url_pattern' in kwargs:
        url_pattern = kwargs['url_pattern']
    if 'lastmod_recent_time' in kwargs:
        if kwargs['lastmod_recent_time'].isdecimal():
            lastmod_recent_time = int(kwargs['lastmod_recent_time'])
        else:
            sys.exit('引数エラー：lastmod_recent_timeは数字(0〜9)のみ使用可。分単位で指定してください。')
    if 'sitemap_continued' in kwargs:
        if kwargs['sitemap_continued'] == 'Yes':
            _crawler_controller = CrawlerControllerModel(mongo)
            crawler_controller_recode = _crawler_controller.find_one(
                {'domain': domain_name})
            if self._crawler_controller_recode == None:
                sys.exit('引数エラー：domain = ' + domain_name +
                         ' は前回のcrawl情報がありません。初回から"sitemap_continued"の使用は不可です。')
            else:
                sitemap_continued = True
                #self._sitemap_contened: dict = { self.name : '', }
        else:
            sys.exit('引数エラー：sitemap_continuedに使用できるのは、"Yes"のみです。')

    ### 項目関連チェック ###
    if 'lastmod_recent_time' in kwargs and 'sitemap_continued' in kwargs:
        sys.exit('引数エラー：lastmod_recent_timeとsitemap_continuedは同時には使えません。')

    return debug_flg, url_term_days, sitemap_term_days, url_pattern, lastmod_recent_time, crawler_controller_recode, sitemap_continued
