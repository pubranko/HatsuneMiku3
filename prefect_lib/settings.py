import os
from datetime import timedelta, timezone
# タイムゾーン
TIMEZONE = timezone(timedelta(hours=9), 'JST')

# メールによる通知をどのLV以上で行うかを指定する。使用できるのは以下の3種類。
#'CRITICAL', 'ERROR', 'WARNING'
NOTICE_LEVEL: str = 'WARNING'

# データ類の保存ベースディレクトリ
DATA_DIR = 'data_dir'

# バックアップファイルを保存するベースディレクトリパス
#BACKUP_BASE_DIR = 'backup_files'
BACKUP_BASE_DIR:str = os.path.join(DATA_DIR, 'backup_files')

DEBUG_FILE_DIR:str = os.path.join(DATA_DIR, 'debug')

DIRECT_CRAWL_FILES_DIR:str = os.path.join(DATA_DIR, 'direct_crawl_files')
