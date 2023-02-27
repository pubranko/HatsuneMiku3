from datetime import datetime
from typing import Any, Optional, Union, Tuple
from pydantic import BaseModel, ValidationError, validator, Field
from urllib.parse import urlparse
from news_crawl.settings import TIMEZONE


class NewsCrawlInput(BaseModel):
    '''
    News Crawlに対する引数用モデル
    引数のチェック及びデータモデルとしての機能を提供する。
    '''
    # News Crawlerの動作モードに関する引数
    debug: bool = Field(False, title="デバックモードフラグ")
    crawl_point_non_update: bool = Field(False, title="クロールポイント更新なしフラグ")

    # クロール開始となる基準時間。指定がなかった場合、現在時刻とする。
    crawling_start_time: datetime = Field(
        datetime.now().astimezone(TIMEZONE), title="クロール開始時間")

    # クロール対象・範囲を指定する任意引数
    lastmod_term_minutes_from: Optional[int] = Field(
        None, title="最終更新期間(分)From")
    lastmod_term_minutes_to: Optional[int] = Field(None, title="最終更新期間(分)To")
    page_span_from: Optional[int] = Field(None, title="ページ範囲")
    page_span_to: Optional[int] = Field(None, title="ページ範囲")
    continued: Optional[bool] = Field(None, title="続きから再開")
    direct_crawl_urls: Optional[list[str]] = Field(
        None, title="直接クロールするURLリスト")
    url_pattern: Optional[str] = Field(None, title="URLパターンによる絞り込み")

    # エラーメッセージを表示させるためのロガー

    def __init__(self, **data: Any):
        super().__init__(**data)

    '''
    クラス変数側の定義順にチェックされる。
    valuesにはチェック済みの値のみが入るため順序は重要。(単項目チェック、関連項目チェックの順で定義するのが良さそう。)
    値がNoneの場合、以下のチェックは動かない。Noneでも動かす場合、「always=True」指定で動かすことができる。例）@validator('aaa', always=True)
    通常上記の型チェックが先に動く。型チェックの前に動かすには「pre=True」指定で動かすことができる。例）@validator('aaa', pre=True, always=True)
    '''
    ##################################
    # 単項目チェック
    ##################################
    @validator('direct_crawl_urls')
    def start_time_check(cls, value: list[str], values: dict) -> list[str]:
        if value:
            for url in value:
                parsed_url = urlparse(url)
                assert len(
                    parsed_url.scheme) > 0, f'引数エラー(direct_crawl_urls): URLとして解析できませんでした {url}'
        return value

    @validator('lastmod_term_minutes_to')
    def lastmod_term_minutes_to_check(cls, value: list[str], values: dict) -> list[str]:
        if value and values['lastmod_term_minutes_from']:
            assert value <= values[
                'lastmod_term_minutes_from'], f'引数エラー : lastmod_term_minutes_from と lastmod_term_minutes_to は、from > toで指定してください。from({values["lastmod_term_minutes_from"]}) : to({value})）'
        return value

    @validator('page_span_to', always=True)
    def page_span_to_check(cls, value: list[str], values: dict) -> list[str]:

        assert ((values['page_span_from'] and value) or
                (not values['page_span_from'] and not value)), f'引数エラー : page_span_from と page_span_to は同時に指定してください。'

        if value and values['page_span_from']:
            assert value > values[
                'page_span_from'], f'引数エラー : page_span_fromとpage_span_toはfrom ≦ toで指定してください。from({values["page_span_from"]}) : to({value})）'

        return value

    ###################################
    #
    ###################################
if __name__ == "__main__":
    params = dict(
        crawling_start_time=datetime(2022, 10, 1, 0, 0, 10),
        debug=True,
        crawl_point_non_update=False,
        lastmod_term_minutes_from=60,
        lastmod_term_minutes_to=0,
        # page_span_from = 2,
        page_span_to=3,
        continued=False,
        direct_crawl_urls=['https://yahoo.co.jp'],
        url_pattern='topic',
        aaaaa='bbbbb',  # 関係無い項目は無視される。
    )
    a = NewsCrawlInput(**params)

    print(a.debug)
    print(a.crawl_point_non_update)
    print(a.lastmod_term_minutes_from)
    print(a.lastmod_term_minutes_to)
    print(a.page_span_from)
    print(a.page_span_to)
    print(a.continued)
    print(a.direct_crawl_urls)
    print(a.url_pattern)
    if a.crawling_start_time:
        aa: datetime = a.crawling_start_time
        print(aa)

    print('=====')

    b = NewsCrawlInput(
        debug=True,
        crawl_point_non_update=False,
        lastmod_term_minutes_from=60,
        lastmod_term_minutes_to=0,
        page_span_from=1,
        page_span_to=3,
        continued=False,
        # direct_crawl_urls = ['https://~','http://aaa~'],
        url_pattern='topic',
    )

    print(b.debug)
    print(b.crawl_point_non_update)
    print(b.lastmod_term_minutes_from)
    print(b.lastmod_term_minutes_to)
    print(b.page_span_from)
    print(b.page_span_to)
    print(b.continued)
    print(b.direct_crawl_urls)
    print(b.url_pattern)

    if b.crawling_start_time:
        bb: datetime = b.crawling_start_time
        print(bb)

    print(b.__dict__)   # クラス変数一括取得
