import os
import sys
from collections.abc import Iterable
from typing import Final
path = os.getcwd()
sys.path.append(path)
from shared.settings import DATA_DIR__DEBUG_FILE_DIR

LASTMOD: Final[str] = 'lastmod'
LOC: Final[str] = 'loc'

def start_request_debug_file_generate(spider_name: str, start_url: str, entries: Iterable, debug: bool):
    '''
    サイトマップ、各カテゴリーの一覧ページ、XMLページなどの情報をデバック用に出力する。
    '''
    if debug:
        path: str = os.path.join(
            DATA_DIR__DEBUG_FILE_DIR, 'start_urls(' + spider_name + ').txt')
        with open(path, 'a') as file:
            for entry in entries:
                entry: dict
                file.write(start_url + ',' + str(entry[LOC]) +
                           ',' + str(entry[LASTMOD]) + '\n')
