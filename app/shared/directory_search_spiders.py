import os
import sys
import re
import glob
import importlib
import inspect
from typing import Any
path = os.getcwd()
sys.path.append(path)


class DirectorySearchSpiders:

    directory_path: str
    spiders_info: dict[str, dict[str, Any]] = {}
    ''' データイメージ
    {'asahi_com_sitemap': {'class_instans': <class 'news_crawl.spiders.asahi_com_sitemap.AsahiComSitemapSpider'>,
                        'class_name': 'AsahiComSitemapSpider',
                        'domain': 'asahi.com',
                        'domain_name': 'asahi_com',
                        'selenium_mode': False,
                        'splash_mode': False},
    〜省略〜
    'sankei_com_sitemap': {'class_instans': <class 'news_crawl.spiders.sankei_com_sitemap.SankeiComSitemapSpider'>,
                            'class_name': 'SankeiComSitemapSpider',
                            'domain': 'sankei.com',
                            'domain_name': 'sankei_com',
                            'selenium_mode': True,
                            'splash_mode': False},}
    '''

    def __init__(self, directory_path: str = 'news_crawl/spiders') -> None:
        self.directory_path = directory_path
        '''
        引数に渡されたパス内にあるスパイダークラスを読み込み、リストにして返す。
        リスト内には、クラス名、インスタンス、クロール先のドメイン名、スパイダー名の辞書を格納。
        '''
        # 引数で渡されたパス内のpythonモジュールファイル名を取得し、パス＋ファイル名を生成する。(fpath)
        # 拡張子を除去＆ファイルパスのセパレータ (Unix系なら'/') を '.' に置き換え
        # モジュールとして読み込み
        for file_path in glob.glob(os.path.join(directory_path, '*.py')):
            mod_path = os.path.splitext(file_path)[0].replace(os.path.sep, '.')
            mod = importlib.import_module(mod_path)

            # 読み込んだモジュールに含まれるクラス名とクラスインスタンスを呼び出す
            for class_name, class_instans in inspect.getmembers(mod, inspect.isclass):
                # 末尾がSpiderのクラスに限定する。
                # ただしSpider用のクラスや継承用のクラスは除外する。
                ptn = re.compile(r'Spider$')  #
                exclusion_list: list = ['ExtensionsSitemapSpider', 'ExtensionsCrawlSpider',
                                        'ExtensionsXmlFeedSpider', 'CloseSpider']
                select_flg: bool = True
                domain: str = ''
                domain_name: str = ''
                spider_name: str = ''
                selenium_mode: bool = False
                splash_mode: bool = False
                if ptn.search(class_name) and \
                        class_name not in exclusion_list:
                    members: dict = class_instans.__dict__
                    if 'allowed_domains' in members:
                        # allowed_domainsリスト内の要素数がゼロの場合、ドメインが設定されていないスパイダーなので除外。
                        if len(members['allowed_domains']) == 0:
                            select_flg = False
                        else:
                            domain = members['allowed_domains'][0]
                            domain_name = members['_domain_name']
                            spider_name = members['name']
                            selenium_mode = members['selenium_mode'] if 'selenium_mode' in members else False
                            splash_mode = members['splash_mode'] if 'splash_mode' in members else False
                    else:
                        select_flg = False
                else:
                    select_flg = False

                # 対象のクラスの場合、クラス名、インスタンス、ドメイン名、スパイダー名のdictを生成
                if select_flg:
                    self.spiders_info[spider_name] = {
                        'class_name': class_name,
                        'class_instans': class_instans,
                        'domain': domain,
                        'domain_name': domain_name,
                        'selenium_mode': selenium_mode,
                        'splash_mode': splash_mode,
                    }

            del mod  # 不要になったモジュールを削除(メモリ節約)

    def spiders_name_list_get(self) -> list:
        '''
        クラス変数に保存されている、スパイダー名のリストを返す。
        '''
        return list(self.spiders_info.keys())

    def spiders_info_list_get(self, target_spiders_name: set) -> list[list[dict[str, Any]]]:
        '''
        引数(spiders_name)で指定された対象スパイダーのスパイダー情報リストを返す。
        ＜データイメージ＞
            result = [[spider_info, spider_info]]
            spider_info = [{'class_instans': *, 'class_name': *, 'domain': *, 'domain_name': *, 'selenium_mode': *, 'splash_mode': *}]
        '''
        result: list[list[dict[str, Any]]] = []
        non_selenium: list[dict[str, Any]] = []
        for spider_name, spider_attr in self.spiders_info.items():
            # 対象スパイダー以外は除外
            if not spider_name in target_spiders_name:
                continue
            non_selenium.append(spider_attr)
        #非seleniumのスパイダーがあった場合
        if len(non_selenium):
            result.append(non_selenium)

        return result

    def separate_spider_using_selenium(self, target_spiders_name: set) -> list[list[dict[str, Any]]]:
        '''
        引数(spiders_name)で指定された対象スパイダーセットに対して、
        seleniumを使用しているスパイダーは単独、それ以外は１つにまとめたリストを返す。
        ＜データイメージ＞
            result = [[spider_info],  # selenium使用
                      [spider_info],  # selenium使用
                      [spider_info, spider_info]]    # selenium未使用
            spider_info = [{'class_instans': *, 'class_name': *, 'domain': *, 'domain_name': *, 'selenium_mode': *, 'splash_mode': *}]
        '''
        result: list[list[dict[str, Any]]] = []
        non_selenium: list[dict[str, Any]] = []
        for spider_name, spider_attr in self.spiders_info.items():
            # 対象スパイダー以外は除外
            if not spider_name in target_spiders_name:
                continue
            # seleniumuを使っている場合は単独、それ以外は非selenium用へ集約
            # if spider_attr['selenium_mode']:
            #     result.append([spider_attr])
            # else:
            #     non_selenium.append(spider_attr)
            non_selenium.append(spider_attr)
        #非seleniumのスパイダーがあった場合
        if len(non_selenium):
            result.append(non_selenium)

        return result
        # result: list = []
        # non_selenium: dict = {}
        # for spider_name, spider_attr in self.spiders_info.items():
        #     # 対象スパイダー以外は除外
        #     if not spider_name in target_spiders_name:
        #         continue
        #     # seleniumuを使っている場合は単独、それ以外は非selenium用へ集約
        #     if spider_attr['selenium_mode']:
        #         result.append({spider_name: spider_attr})
        #     else:
        #         non_selenium[spider_name] = spider_attr
        # #非seleniumのスパイダーがあった場合
        # if len(non_selenium):
        #     result.append(non_selenium)

        # return result

if __name__ == "__main__":
    # execute only if run as a script
    directory_search_spiders = DirectorySearchSpiders()
    spiders_info = directory_search_spiders.spiders_info
    separate_spider_using_selenium = directory_search_spiders.separate_spider_using_selenium(
        set(directory_search_spiders.spiders_name_list_get()))
    from pprint import pprint
    pprint(spiders_info)
    print('\n========================================\n')
    pprint(separate_spider_using_selenium)
