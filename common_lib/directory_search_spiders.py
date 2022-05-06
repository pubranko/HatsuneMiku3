import os
import sys
import re
import glob
import importlib
import inspect
from typing import Any
path = os.getcwd()
sys.path.append(path)


def directory_search_spiders(directory_path: str = 'news_crawl/spiders') -> list:
    '''
    引数に渡されたパス内にあるスパイダークラスを読み込み、リストにして返す。
    リスト内には、クラス名、インスタンス、クロール先のドメイン名、スパイダー名の辞書を格納。
    '''
    class_list: list = []

    # 引数で渡されたパス内のpythonモジュールファイル名を取得し、パス＋ファイル名を生成する。(fpath)
    # 拡張子を除去＆ファイルパスのセパレータ (Unix系なら'/') を '.' に置き換え
    # モジュールとして読み込み
    for file_path in glob.glob(os.path.join(directory_path, '*.py')):
        mod_path = os.path.splitext(file_path)[0].replace(os.path.sep, '.')
        mod = importlib.import_module(mod_path)

        # 読み込んだモジュールに含まれるクラス名とクラスインスタンスを呼び出す
        for class_name, class_instans in inspect.getmembers(mod, inspect.isclass):

            print(f'=== 開始{class_name}')
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
                        select_flg: bool = False
                    else:
                        domain = members['allowed_domains'][0]
                        domain_name = members['_domain_name']
                        spider_name = members['name']
                        selenium_mode = members['selenium_mode'] if 'selenium_mode' in members else False
                        splash_mode = members['splash_mode'] if 'splash_mode' in members else False
                else:
                    select_flg: bool = False
            else:
                select_flg = False

            # 対象のクラスの場合、クラス名、インスタンス、ドメイン名、スパイダー名のdictを生成
            if select_flg:
                class_list.append({
                    'class_name': class_name,
                    'class_instans': class_instans,
                    'domain': domain,
                    'domain_name': domain_name,
                    'spider_name': spider_name,
                    'selenium_mode': selenium_mode,
                    'splash_mode': splash_mode,
                })

        del mod  # 不要になったモジュールを削除(メモリ節約)

    return class_list

if __name__ == "__main__":
    # execute only if run as a script
    class_list = directory_search_spiders()
    from pprint import pprint
    pprint(class_list)
