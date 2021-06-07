import os
from collections.abc import Iterable


def start_request_debug_file_generate(spider_name: str, start_url: str, entries: Iterable, kwargs_save: dict):
    '''
    サイトマップ、各カテゴリーの一覧ページ、XMLページなどの情報をデバック用に出力する。
    '''
    if 'debug' in kwargs_save:
        path: str = os.path.join(
            'debug', 'start_urls(' + spider_name + ').txt')
        with open(path, 'a') as file:
            for entry in entries:
                entry: dict
                file.write(start_url + ',' + entry['loc'] +
                           ',' + entry['lastmod'] + '\n')