from datetime import timedelta, timezone
# タイムゾーン
TIMEZONE = timezone(timedelta(hours=9), 'JST')

# メールによる通知をどのLV以上で行うかを指定する。使用できるのは以下の3種類。
#'CRITICAL', 'ERROR', 'WARNING'
NOTICE_LEVEL: str = 'WARNING'

# バックアップファイルを保存するベースディレクトリパス
BACKUP_BASE_DIR = 'backup_files'

# データ類の保存ベースディレクトリ
DATA_DIR = 'data_dir'