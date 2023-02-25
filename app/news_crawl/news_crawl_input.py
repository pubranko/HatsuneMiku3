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
    ### News Crawlerの動作モードに関する引数
    debug: bool = Field(False, title="デバックモードフラグ")
    crawl_point_non_update: bool = Field(False, title="クロールポイント更新なしフラグ")

    ### クロール開始となる基準時間。指定がなかった場合、現在時刻とする。
    crawling_start_time:datetime = Field(datetime.now().astimezone(TIMEZONE), title="クロール開始時間")

    ### クロール対象・範囲を指定する任意引数
    lastmod_term_minutes_from:Optional[int] = Field(None, title="最終更新期間(分)From")
    lastmod_term_minutes_to:Optional[int] = Field(None, title="最終更新期間(分)To")
    page_span_from: Optional[int] = Field(None, title="ページ範囲")
    page_span_to: Optional[int] = Field(None, title="ページ範囲")
    continued:Optional[bool]  = Field(None, title="続きから再開")
    direct_crawl_urls: Optional[list[str]] = Field(None, title="直接クロールするURLリスト")
    url_pattern: Optional[str] = Field(None, title="URLパターンによる絞り込み")

    ### エラーメッセージを表示させるためのロガー

    def __init__(self, **data: Any):
        super().__init__(**data)

    '''
    定義順にチェックされる。
    valuesにはチェック済みの値のみが入るため順序は重要。(単項目チェック、関連項目チェックの順で定義するのが良さそう。)
    '''
    ##################################
    # 単項目チェック
    ##################################
    @validator('direct_crawl_urls')
    def start_time_check(cls, value: list[str], values: dict) -> list[str]:
        for url in value:
            parsed_url = urlparse(url)
            assert len(parsed_url.scheme) > 0, f'引数エラー(direct_crawl_urls): URLとして解析できませんでした {url}'

        return value

    ###################################
    # 関連項目チェック
    ###################################

    ###################################
    # 
    ###################################
    # url_term_days   廃止機能だ
    # sitemap_term_days   廃止機能だ
    # lastmod_recent_time   廃止機能だ
    # category_urls   廃止機能だ
    # error_notice   廃止機能だ

    # crawling_start_time
    # debug
    # continued
    # pages
    # lastmod_period_minutes
    # crawl_point_non_update
    # direct_crawl_urls
    # url_pattern

if __name__ == "__main__":
    params = dict(
        crawling_start_time = datetime(2022,10,1,0,0,10),
        debug = True,
        crawl_point_non_update = False,
        lastmod_term_minutes_from = 60,
        lastmod_term_minutes_to = 0,
        page_span_from = 1,
        # page_span_to = 3,
        continued = False,
        direct_crawl_urls = ['https://yahoo.co.jp'],
        url_pattern = 'topic',
        aaaaa = 'bbbbb',            #関係無い項目は無視される。
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
        aa:datetime = a.crawling_start_time
        print(aa)

    print('=====')

    b = NewsCrawlInput(
        debug = True,
        crawl_point_non_update = False,
        lastmod_term_minutes_from = 60,
        lastmod_term_minutes_to = 0,
        page_span_from = 1,
        page_span_to = 3,
        continued = False,
        # direct_crawl_urls = ['https://~','http://aaa~'],
        url_pattern = 'topic',
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
        bb:datetime = b.crawling_start_time
        print(bb)

    print(b.__dict__)   # クラス変数一括取得