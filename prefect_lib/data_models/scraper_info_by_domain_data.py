from __future__ import annotations
import os
import glob
from typing import Any
import pandas as pd
from pydantic import BaseModel, ValidationError, validator, Field
from pydantic.main import ModelMetaclass


class ScraperInfoByDomainData(BaseModel):
    '''
    ドメイン別スクレイパー情報用のデータクラス
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
                    elif not all((s in pattern_info.keys()) for s in ['pattern', 'css_selecter', 'priority', 'register_date']):
                        raise ValueError(
                            f'不正データ。パターン情報内にpattern,css_selecter,priority,register_dateが揃って定義されていません。({pattern_info})')
        return value

    ###################################
    # 関連項目チェック
    ###################################

    #####################################
    # カスタマイズデータ
    #####################################
    def domain_get(self) -> str:
        return self.scraper['domain']

    def making_into_a_table_format(self) -> list[dict[str,Any]]:
        '''ドメイン別スクレイパー情報を表形式へ加工'''
        result:list = []
        scrape_items: dict = self.scraper["scrape_items"]
        for scraper_item_key, scraper_item_value in scrape_items.items():

            for pattern_info in scraper_item_value:
                result.append({
                    'domain': self.scraper['domain'],
                    'scraper_item': scraper_item_key,
                    'pattern': pattern_info['pattern'],
                    'priority': pattern_info['priority'],
                    #'count_of_use': 0,
                })
        return result


'''データイメージ
{
    "_id": ObjectID("62542b32efbb8d4ef6de0356"),
    "domain": "yomiuri.co.jp",
    "scrape_items": {
        "title_scraper": [
            {
                "pattern": 2,
                "css_selecter": "head > title",
                "priority": 2,
                "register_date": "2022-04-16T14:00:00+09:00"
            },
            {
                "pattern": 1,
                "css_selecter": "title",
                "priority": 1,
                "register_date": "2022-04-16T14:00:00+09:00"
            }
        ],
        "article_scraper": [
            {
                "pattern": 4,
                "css_selecter": "div.p-main-contents > p",
                "priority": 4,
                "register_date": "2022-04-16T14:00:00+09:00"
            },
            {
                "pattern": 3,
                "css_selecter": "div.p-main-contents > p[class^=par]",
                "priority": 3,
                "register_date": "2022-04-16T14:00:00+09:00"
            },
            {
                "pattern": 2,
                "css_selecter": "div.p-main-contents > p[iarticle_selecterrop=articleBody]",
                "priority": 2,
                "register_date": "2022-04-16T14:00:00+09:00"
            },
            {
                "pattern": 1,
                "css_selecter": "div.main-contents > p[iarticle_selecterrop=articleBody]",
                "priority": 1,
                "register_date": "2022-04-16T14:00:00+09:00"
            }
        ],
        "publish_date_scraper": [
            {
                "pattern": 2,
                "css_selecter": "head > meta[property=\"article:modified_time\"]",
                "priority": 2,
                "register_date": "2022-04-16T14:00:00+09:00"
            },
            {
                "pattern": 1,
                "css_selecter": "head > meta[property=\"article:published_time\"]",
                "priority": 1,
                "register_date": "2022-04-16T14:00:00+09:00"
            }
        ]
    }
}

'''
