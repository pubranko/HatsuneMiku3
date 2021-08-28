from datetime import datetime
from dateutil import tz
from prefect_lib.settings import TIMEZONE

def timezone_recovery(dt:datetime) -> datetime:
    '''
    MongoDBから取得したISODate型はタイムゾーンが抜け落ちている。
    それの補正とタイムゾーンの付与を行ったdatetime型を返す。
    MongoDBから取得したISODate型の項目を使う場合、基本的に当関数を通じて使用すること。
    '''
    UTC = tz.gettz("UTC")
    dt = dt.replace(tzinfo=UTC)
    dt = dt.astimezone(TIMEZONE)
    return dt