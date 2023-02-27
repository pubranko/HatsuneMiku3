from typing import Optional
from datetime import datetime, timedelta


class LastmodTermSkipCheck(object):

    lastmod_term_datetime_from: Optional[datetime] = None
    lastmod_term_datetime_to: Optional[datetime] = None

    # def __init__(self, spider, start_time: datetime, kwargs: dict) -> None:
    def __init__(self, spider, start_time: datetime, lastmod_term_minutes_from: Optional[int], lastmod_term_minutes_to: Optional[int]) -> None:
        '''
        lastmodの期間指定がある場合、datetime形式にしてクラス変数に保存
        '''
        if lastmod_term_minutes_from or lastmod_term_minutes_to:
            if lastmod_term_minutes_from:
                self.lastmod_term_datetime_from = \
                    start_time - timedelta(minutes=int(lastmod_term_minutes_from))
                spider.logger.info(
                    f'=== lastmod_term_minutes_from より計算した時間: {self.lastmod_term_datetime_from.isoformat()}')
            if lastmod_term_minutes_to:
                self.lastmod_term_datetime_to = \
                    start_time - timedelta(minutes=int(lastmod_term_minutes_to))
                spider.logger.info(
                    f'=== lastmod_term_minutes_to より計算した時間: {self.lastmod_term_datetime_to.isoformat()}')

    def skip_check(self, lastmod: datetime) -> bool:
        '''
        lastmodの期間指定があり、指定期間内であればクロール対象としてスキップ対象外(False)を返す。
        期間外の場合はスキップ対象(True)を返す。
        '''
        skip_flg: bool = False
        if self.lastmod_term_datetime_from:
            if lastmod < self.lastmod_term_datetime_from:
                skip_flg = True
        if self.lastmod_term_datetime_to:
            if lastmod > self.lastmod_term_datetime_to:
                skip_flg = True

        return skip_flg
