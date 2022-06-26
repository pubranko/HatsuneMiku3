import os
from datetime import timedelta, timezone


import scrapy
# タイムゾーン
TIMEZONE = timezone(timedelta(hours=9), 'JST')

# メールによる通知をどのLV以上で行うかを指定する。使用できるのは以下の3種類。
#'CRITICAL', 'ERROR', 'WARNING'
NOTICE_LEVEL: str = 'WARNING'

# データ類の保存ベースディレクトリ
# DATA_DIR = 'data_dir'
# DATA_DIR = os.environ['PREFECT_DATA_DIR_PATH']
DATA_DIR = os.path.abspath(os.environ['PREFECT_DATA_DIR_PATH'])

# DATA_DIR = 'data_dir'
DATA_DIR_LOGS = os.path.join(DATA_DIR, 'logs')
# バックアップファイルを保存するベースディレクトリパス
#BACKUP_BASE_DIR = 'backup_files'
BACKUP_BASE_DIR:str = os.path.join(DATA_DIR, 'backup_files')
# デバック用ファイルの保存先
DEBUG_FILE_DIR:str = os.path.join(DATA_DIR, 'debug')
# ダイレクトクロール用のファイルの格納先
DIRECT_CRAWL_FILES_DIR:str = os.path.join(DATA_DIR, 'direct_crawl_files')
# ドメイン別スクレイパーファイルの格納先
SCRAPER_INFO_BY_DOMAIN_DIR:str = os.path.join(DATA_DIR, 'scraper_info_by_domain')