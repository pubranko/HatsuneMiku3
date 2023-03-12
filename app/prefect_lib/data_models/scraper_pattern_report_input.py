from calendar import month
from copy import deepcopy
import re
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from xml.dom import minicompat
from dateutil import parser
from typing import Any, Union, Optional, Tuple, Final
from pydantic import BaseModel, ValidationError, validator, Field
from pydantic.main import ModelMetaclass
from shared.settings import TIMEZONE

############################################
# 定数
# ※クラス内で定義したかったが、その場合クラス内で参照できなかった。
#   次善の策としてモジュール定数側で定義。
############################################
CONST__START_TIME: Final[str] = 'start_time'
CONST__REPORT_TERM: Final[str] = 'report_term'
CONST__BASE_DATE: Final[str] = 'base_date'
CONST__REPORT_TERM__DAILY: Final[str] = 'daily'
CONST__REPORT_TERM__WEEKLY: Final[str] = 'weekly'
CONST__REPORT_TERM__MONTHLY: Final[str] = 'monthly'
CONST__REPORT_TERM__YEARLY: Final[str] = 'yearly'


class ScraperPatternReportInput(BaseModel):
    '''
    start_time,report_term,base_date
    '''
    start_time: datetime = Field(..., title="開始時間")
    report_term: str = Field(..., title="レポート期間")
    base_date: Optional[datetime] = None

    #####################
    # 定数
    #####################
    START_TIME: str = Field(CONST__START_TIME, const=True)
    '''定数: start_time '''
    REPORT_TERM: str = Field(CONST__REPORT_TERM, const=True)
    '''定数: report_term '''
    BASE_DATE: str = Field(CONST__BASE_DATE, const=True)
    '''定数: base_date '''
    REPORT_TERM__DAILY: str = Field(CONST__REPORT_TERM__DAILY, const=True)
    '''区分値: report_term__daily '''
    REPORT_TERM__WEEKLY: str = Field(CONST__REPORT_TERM__WEEKLY, const=True)
    '''区分値: rreport_term__weekly '''
    REPORT_TERM__MONTHLY: str = Field(CONST__REPORT_TERM__MONTHLY, const=True)
    '''区分値: rreport_term__monthly '''
    REPORT_TERM__YEARLY: str = Field(CONST__REPORT_TERM__YEARLY, const=True)
    '''区分値: rreport_term__yearly '''


    def __init__(self, **data: Any):
        '''
        あとで
        '''
        super().__init__(**data)

    '''
    定義順にチェックされる。
    valuesにはチェック済みの値のみが入るため順序は重要。(単項目チェック、関連項目チェックの順で定義するのが良さそう。)
    '''
    ##################################
    # 単項目チェック、省略時の値設定
    ##################################
    @validator(CONST__START_TIME)
    def start_time_check(cls, value: datetime, values: dict) -> datetime:
        if value:
            assert isinstance(value, datetime), '日付型以外がエラー'
        return value

    @validator(CONST__REPORT_TERM)
    def report_term_check(cls, value: str, values: dict) -> str:
        if value:
            assert isinstance(value, str), '文字列型以外がエラー'
            # 本番には3ヶ月以上のデータ残さないからyearlyはいらないかも、、、
            if value not in [CONST__REPORT_TERM__DAILY, CONST__REPORT_TERM__WEEKLY, CONST__REPORT_TERM__MONTHLY, CONST__REPORT_TERM__YEARLY]:
                raise ValueError(
                    f'レポート期間の指定ミス。{CONST__REPORT_TERM__DAILY}, {CONST__REPORT_TERM__WEEKLY}, {CONST__REPORT_TERM__MONTHLY}, {CONST__REPORT_TERM__YEARLY}で入力してください。')
        return value

    @validator(CONST__BASE_DATE)
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

        if self.report_term == CONST__REPORT_TERM__DAILY:
            base_date_from = base_date_to - relativedelta(days=1)
        elif self.report_term == CONST__REPORT_TERM__WEEKLY:
            base_date_from = base_date_to - relativedelta(weeks=1)
        elif self.report_term == CONST__REPORT_TERM__MONTHLY:
            base_date_from = base_date_to - relativedelta(months=1)
        else:
            base_date_from = base_date_to - relativedelta(years=1)

        return (base_date_from, base_date_to)
