from calendar import month
import re
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from xml.dom import minicompat
from dateutil import parser
from typing import Any, Union, Optional, Tuple
from pydantic import BaseModel, ValidationError, validator, Field
from pydantic.main import ModelMetaclass
from common_lib.common_settings import TIMEZONE


class StatsInfoCollectInput(BaseModel):
    '''
    start_time,report_term,base_date
    '''
    start_time: datetime = Field(..., title="開始時間")
    base_date: Optional[datetime] = None

    def __init__(self, **data: Any):
        super().__init__(**data)

    '''
    定義順にチェックされる。
    valuesにはチェック済みの値のみが入るため順序は重要。(単項目チェック、関連項目チェックの順で定義するのが良さそう。)
    '''
    ##################################
    # 単項目チェック、省略時の値設定
    ##################################
    @validator('start_time')
    def start_time_check(cls, value: str, values: dict) -> str:
        if value:
            assert isinstance(value, datetime), '日付型以外がエラー'
        return value

    @validator('base_date')
    def base_date_check(cls, value: Optional[datetime], values: dict) -> Optional[datetime]:
        if value:
            assert isinstance(value, datetime), '日時型以外がエラー'
        return value

    ###################################
    # 関連項目チェック
    ###################################

    #####################################
    # カスタマイズデータ
    #####################################
    def base_date_get(self) -> Tuple[datetime, datetime]:
        '''
        レポート期間(report_term)と基準日(base_date)を基に基準期間(base_date_from, base_date_to)を取得する。
        ※基準日(base_date)=基準期間to(base_date_to)となる。
        '''
        if self.base_date:
            base_date_from = self.base_date
        else:
            base_date_from = self.start_time.replace(
                hour=0, minute=0, second=0, microsecond=0) - relativedelta(days=1)

        base_date_to = base_date_from + relativedelta(days=1)

        return (base_date_from, base_date_to)
