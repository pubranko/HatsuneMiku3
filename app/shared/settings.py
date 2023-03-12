import os
from typing import Final
from datetime import timedelta, timezone
from decouple import config, AutoConfig
# .envファイルが存在するパスを指定。実行時のカレントディレクトリに.envを配置している場合、以下の設定不要。
# config = AutoConfig(search_path="./shared")


import scrapy
'''タイムゾーン'''
TIMEZONE = timezone(timedelta(hours=9), 'JST')

'''データ類の保存ベースディレクトリ'''
DATA_DIR = os.path.abspath(str(config('PREFECT__DATA_DIR_PATH', default='data_dir')))

'''ログの一時保存先'''
DATA_DIR__LOGS = os.path.join(DATA_DIR, 'logs')
'''バックアップファイルを保存するベースディレクトリパス'''
DATA_DIR__BACKUP_BASE_DIR:str = os.path.join(DATA_DIR, 'backup_files')
'''デバック用ファイルの保存先'''
DATA_DIR__DEBUG_FILE_DIR:str = os.path.join(DATA_DIR, 'debug')
'''ダイレクトクロール用のファイルの格納先'''
DATA_DIR__DIRECT_CRAWL_FILES_DIR:str = os.path.join(DATA_DIR, 'direct_crawl_files')
'''ドメイン別スクレイパーファイルの格納先'''
DATA_DIR__SCRAPER_INFO_BY_DOMAIN_DIR:str = os.path.join(DATA_DIR, 'scraper_info_by_domain')

'''クロール時にログインが必要なサイトのログイン情報の格納先ディレクトリ'''
DATA_DIR__LOGIN_INFO:str = os.path.join(DATA_DIR, 'login_info')
'''クロール時にログインが必要なサイトのログイン情報の格納先ファイル名(yml)'''
DATA_DIR__LOGIN_INFO_YML:str = 'login_info.yml'

'''prefect_libのタスクを格納するディレクトリ'''
PREFECT_LIB__TASK_DIR: Final[str] = 'prefect_lib/task'

