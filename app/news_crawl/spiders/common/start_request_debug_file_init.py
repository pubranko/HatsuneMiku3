import os
import sys
from scrapy.spiders import Spider
path = os.getcwd()
sys.path.append(path)
from shared.settings import DEBUG_FILE_DIR


def start_request_debug_file_init(spider: Spider, debug: bool):
    '''
    サイトマップ、各カテゴリーの一覧ページ、XMLページなどの情報をデバック用に初期化（空ファイル化）する。
    '''
    if debug:
        spider.logger.info(f'=== debugモード ON: {spider.name}')
        # デバック用のファイルを初期化
        path = os.path.join(
            DEBUG_FILE_DIR, 'start_urls(' + str(spider.name) + ').txt')
        with open(path, 'w') as file:
            pass
