import os
from scrapy.spiders import Spider


def start_request_debug_file_init(spider: Spider, kwargs_save: dict):
    '''
    サイトマップ、各カテゴリーの一覧ページ、XMLページなどの情報をデバック用に初期化（空ファイル化）する。
    '''
    if 'debug' in kwargs_save and kwargs_save['debug'] == 'Yes':
        spider.logger.info(f'=== debugモード ON: {spider.name}')
        # デバック用のファイルを初期化
        path = os.path.join(
            'debug', 'start_urls(' + str(spider.name) + ').txt')
        with open(path, 'w') as file:
            pass
