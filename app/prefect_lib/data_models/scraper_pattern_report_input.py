from calendar import month
from copy import deepcopy
import re
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from xml.dom import minicompat
from dateutil import parser
from typing import Any, Union, Optional, Tuple
from pydantic import BaseModel, ValidationError, validator, Field
from pydantic.main import ModelMetaclass
from common_lib.common_settings import TIMEZONE


class ScraperPatternReportInput(BaseModel):
    '''
    start_time,report_term,base_date
    '''
    start_time: datetime = Field(..., title="開始時間")
    report_term: str = Field(..., title="レポート期間")
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

    @validator('report_term')
    def report_term_check(cls, value: str, values: dict) -> str:
        if value:
            assert isinstance(value, str), '文字列型以外がエラー'
            # 本番には3ヶ月以上のデータ残さないからyearlyはいらないかも、、、
            if value not in ['daily', 'weekly', 'monthly', 'yearly']:
                raise ValueError(
                    'レポート期間の指定ミス。daily, weekly, monthly, yearlyで入力してください。')
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
        start_time: datetime = self.start_time
        #base_date = self.base_date
        if self.base_date:
            base_date_to = self.base_date
        else:
            base_date_to = start_time.replace(
                hour=0, minute=0, second=0, microsecond=0)

        if self.report_term == 'daily':
            base_date_from = base_date_to - relativedelta(days=1)
        elif self.report_term == 'weekly':
            base_date_from = base_date_to - relativedelta(weeks=1)
        elif self.report_term == 'monthly':
            base_date_from = base_date_to - relativedelta(months=1)
        else:
            base_date_from = base_date_to - relativedelta(years=1)

        return (base_date_from, base_date_to)
