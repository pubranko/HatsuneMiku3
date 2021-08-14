# タイムゾーン
from datetime import timedelta, timezone
TIMEZONE = timezone(timedelta(hours=9), 'JST')

# メールによる通知をどのLV以上で行うかを指定する。使用できるのは以下の3種類。
#'CRITICAL', 'ERROR', 'WARNING'
NOTICE_LEVEL:str = 'WARNING'