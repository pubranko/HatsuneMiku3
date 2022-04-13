from __future__ import annotations
import os
import glob
from pydantic import BaseModel, ValidationError, validator, Field
from pydantic.main import ModelMetaclass
from prefect_lib.settings import TIMEZONE


class ScraperInfoByDomainData(BaseModel):
    '''
    start_time,report_term,base_date
    '''
    scraper: dict = Field(..., title="スクレイパー情報")

    def __init__(self, **data: dict):
        super().__init__(**data)

    '''
    定義順にチェックされる。
    valuesにはチェック済みの値のみが入るため順序は重要。(単項目チェック、関連項目チェックの順で定義するのが良さそう。)
    '''
    ##################################
    # 単項目チェック、省略時の値設定
    ##################################

    @validator('scraper')
    def scraper_domain_check(cls, value: dict, values: dict) -> dict:
        if not 'domain' in value:
            raise ValueError(
                f'不正データ。ドメイン(domain)が定義されていません。{value}')
        elif not type(value['domain']) is str:
            raise ValueError(
                f'不正データ。ドメイン(domain)の値が文字列型以外はエラー({type(value["domain"])})')
        return value

    @validator('scraper')
    def scraper_items_check(cls, value: dict, values: dict) -> dict:
        if not 'scrape_items' in value:
            raise ValueError(
                f'不正データ。スクレイプアイテム(scrape_items)が定義されていません。({value})')
        elif not type(value['scrape_items']) is dict:
            raise ValueError(
                f'不正データ。スクレイプアイテム(scrape_items)の値が辞書型以外はエラー。{type(value["scrape_items"])}')
        elif len(value['scrape_items']) == 0:
            raise ValueError(
                f'不正データ。スクレイプアイテム(scrape_items)内のスクレイパーが定義されていません。{len(value["scrape_items"])}')
        else:
            for scrape_item_key, scrape_item_value in value['scrape_items'].items():
                path = os.path.join('prefect_lib', 'scraper',
                                    f'{scrape_item_key}.py')
                if len(glob.glob(path)) == 0:
                    raise ValueError(
                        f'不正データ。スクレイプアイテム(scrape_items)で指定されたスクレイパーは登録されていないため使用できません。({scrape_item_key})')
                elif not type(scrape_item_value) is list:
                    raise ValueError(
                        f'不正データ。スクレイパーの値がリスト型以外はエラー。({scrape_item_value})')
                elif len(scrape_item_value) == 0:
                    raise ValueError(
                        f'不正データ。スクレイパー内のパターンが定義されていません。({scrape_item_value})')

                for pattern_info in scrape_item_value:
                    if not type(pattern_info) is dict:
                        raise ValueError(
                            f'不正データ。パターン情報の値が辞書型以外はエラー。({scrape_item_value})')
                    elif not all((s in pattern_info.keys()) for s in ['pattern','css_selecter']):
                        raise ValueError(
                            f'不正データ。パターン情報内にpatternとcss_selecterが揃って定義されていません。({pattern_info})')

        return value

    ###################################
    # 関連項目チェック
    ###################################

    #####################################
    # カスタマイズデータ
    #####################################
