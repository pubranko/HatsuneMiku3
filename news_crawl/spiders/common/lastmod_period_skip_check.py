from typing import Union
from datetime import datetime, timedelta


class LastmodPeriodMinutesSkipCheck(object):

    lastmod_period_minutes_from: Union[datetime, None] = None
    lastmod_period_minutes_to: Union[datetime, None] = None

    def __init__(self, spider, start_time: datetime, kwargs: dict) -> None:
        '''
        lastmodの期間指定がある場合、datetime形式にしてクラス変数に保存
        ※kwargs['lastmod_period_minutes']=[from,to]
        '''
        if 'lastmod_period_minutes' in kwargs and kwargs['lastmod_period_minutes']:             # lastmod期間指定あり
            lastmod_period_minutes_list = str(kwargs['lastmod_period_minutes']).split(',')
            if not lastmod_period_minutes_list[0] == '':
                self.lastmod_period_minutes_from = start_time - \
                    timedelta(minutes=int(lastmod_period_minutes_list[0]))
                spider.logger.info(
                    f'=== lastmod_period_minutesのfromより計算した時間: {self.lastmod_period_minutes_from.isoformat()}')
            if not lastmod_period_minutes_list[1] == '':
                self.lastmod_period_minutes_to = start_time - \
                    timedelta(minutes=int(lastmod_period_minutes_list[1]))
                spider.logger.info(
                    f'=== lastmod_period_minutesのtoより計算した時間: {self.lastmod_period_minutes_to.isoformat()}')

    def skip_check(self, lastmod: datetime) -> bool:
        '''
        lastmodの期間指定があり、指定期間内であればクロール対象としてスキップ対象外(False)を返す。
        期間外の場合はスキップ対象(True)を返す。
        '''
        skip_flg: bool = False
        if self.lastmod_period_minutes_from:
            if lastmod < self.lastmod_period_minutes_from:
                skip_flg = True
        if self.lastmod_period_minutes_to:
            if lastmod > self.lastmod_period_minutes_to:
                skip_flg = True

        return skip_flg
