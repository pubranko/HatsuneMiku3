import os
from datetime import timedelta, timezone
from decouple import config, AutoConfig
# .envファイルが存在するパスを指定。実行時のカレントディレクトリに.envを配置している場合、以下の設定不要。
# config = AutoConfig(search_path="./shared")


import scrapy
# タイムゾーン
TIMEZONE = timezone(timedelta(hours=9), 'JST')

# データ類の保存ベースディレクトリ
DATA_DIR = os.path.abspath(str(config('PREFECT__DATA_DIR_PATH', default='data_dir')))

# ログの一時保存先
DATA_DIR_LOGS = os.path.join(DATA_DIR, 'logs')
# バックアップファイルを保存するベースディレクトリパス
BACKUP_BASE_DIR:str = os.path.join(DATA_DIR, 'backup_files')
# デバック用ファイルの保存先
DEBUG_FILE_DIR:str = os.path.join(DATA_DIR, 'debug')
# ダイレクトクロール用のファイルの格納先
DIRECT_CRAWL_FILES_DIR:str = os.path.join(DATA_DIR, 'direct_crawl_files')
# ドメイン別スクレイパーファイルの格納先
SCRAPER_INFO_BY_DOMAIN_DIR:str = os.path.join(DATA_DIR, 'scraper_info_by_domain')