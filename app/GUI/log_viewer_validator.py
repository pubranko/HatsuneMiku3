import re
from datetime import datetime
from dateutil import parser
from typing import Any, Union
from pydantic import BaseModel, ValidationError, validator,Field
from pydantic.main import ModelMetaclass
from common_lib.common_settings import TIMEZONE

class LogViewerValidator(BaseModel):
    date_from: str = ''
    time_from: str = ''
    date_to: str = ''
    time_to: str = ''
    record_type: list = []
    log_level_value: int = 9

    '''
    定義順にチェックされる。
    valuesにはチェック済みの値のみが入るため順序は重要。(単項目チェック、関連項目チェックの順で定義するのが良さそう。)
    '''
    ################################
    # 単項目チェック
    ################################
    @validator('date_from','date_to')
    def date_check(cls, value:str, values:dict) -> Union[int,str]:
        '''date_fromのチェックを行う。正常の場合、半角数値に変換して返す。'''
        if value:
            assert value.isdecimal(),'数字以外が含まれている'
            #int(value)  #全て半角数値へ変換
            pattern = re.compile(r'[0-9]{4}(0[1-9]|1[0-2])(0[1-9]|[12][0-9]|3[01])')
            assert pattern.match(value),'yyyymmdd以外の入力不可'
            try:
                parser.parse(value)
            except:
                raise ValueError('存在しない月日')
            return value
        else:
            return value

    @validator('time_from','time_to')
    def time_check(cls, value:str, values:dict) -> Union[int,str]:
        if value:
            assert value.isdecimal(),'数字以外が含まれている'
            #int(value)  #全て半角数値へ変換
            pattern = re.compile(r'([01][0-9]|2[0-3])[0-5][0-9][0-5][0-9]')
            assert pattern.match(value),'hhmmss以外の入力不可'
            return value
        else:
            return value

    @validator('record_type')
    def record_type_check(cls, value:list, values:dict) -> list:
        if value:
            assert isinstance(value,list),'リスト型以外がエラー'
        return value

    @validator('log_level_value')
    def log_level_value_check(cls, value:list, values:dict) -> list:
        if value:
            assert isinstance(value,int),'整数型以外がエラー'
        return value

    ###################################
    # 関連項目チェック
    ###################################
    @validator('time_from')
    def time_from_check(cls, value:int, values:dict) -> int:
        if value:
            assert values['date_from'],'時間を指定する場合、日付入力は必須です'
        return value

    @validator('time_to')
    def time_to_check(cls, value:int, values:dict) -> int:
        if value:
            assert values['date_to'],'時間を指定する場合、日付入力は必須です'
        return value

    #####################################
    # 型変換
    #####################################
    def datetime_from(self):
        return parser.parse(self.date_from + self.time_from).astimezone(TIMEZONE)
    def datetime_to(self):
        return parser.parse(self.date_to + self.time_to).astimezone(TIMEZONE)