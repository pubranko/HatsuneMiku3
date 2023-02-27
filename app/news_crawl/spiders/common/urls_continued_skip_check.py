import os
import sys
from typing import Optional
path = os.getcwd()
sys.path.append(path)


class UrlsContinuedSkipCheck(object):
    '''
    前回の続きからクロールさせるためのチェックを行う。
    '''
    crawl_point_save: dict = {}
    last_time_urls: list = []
    continued:Optional[bool]
    skip_flg: bool = False
    check_count: int = 10

    def __init__(self, crawl_point: dict, base_url: str, continued: Optional[bool]) -> None:
        '''
        前回の続きの指定がある場合、前回のクロールポイントの5件のurlをクラス変数へ保存する。
        '''
        # 前回からの続きの指定がある場合、前回の１ページ目の５件のURLを取得する。
        self.continued = continued
        if self.continued:
            if base_url in crawl_point:
                self.last_time_urls = [
                    _['loc'] for _ in crawl_point[base_url]['urls']]

    def skip_check(self, url:str) -> bool:
        '''
        前回の続きの指定がある場合、前回のクロールポイントの5件のurlまで読み込みが完了しているか判定を行う。
        ・前回のクロールポイントまで読み込みが完了していた場合、引数のurlをスキップ対象（True）とする。
        ・上記以外の場合、引数のurlをスキップ対象外（False）とする。
        ・ただし、前回の続きの指定がない場合、常にスキップ対象外（False）を返す。
        ※前回のクロールポイントには10件のurlがあるが、url取得中に更新されトップページへ移動している可能性がある。
          無限ループに陥らないように5/10件で完了とさせる。
        '''
        if self.continued:
            if len(self.last_time_urls) <= self.check_count / 2:
                self.skip_flg = True

            if url in self.last_time_urls:
                self.last_time_urls.remove(url)

        return self.skip_flg

