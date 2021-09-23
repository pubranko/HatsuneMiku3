import os
import sys
from typing import Any
from datetime import datetime
path = os.getcwd()
sys.path.append(path)
from common_lib.timezone_recovery import timezone_recovery


class UrlsContinuedSkipCheck(object):
    '''
    続きからクロール
    '''
    crawl_point_save: dict = {}
    last_time_urls: list = []
    kwargs_save: dict = {}
    crwal_flg: bool = True

    def __init__(self, crawl_point: dict, base_url: str, kwargs: dict) -> None:
        '''
        前回の続きの指定がある場合、前回のクロールポイントの５件のurlをクラス変数へ保存する。
        '''
        self.kwargs_save = kwargs
        # 前回からの続きの指定がある場合、前回の１ページ目の５件のURLを取得する。
        if 'continued' in self.kwargs_save:
            if base_url in crawl_point:
                self.last_time_urls = [
                    _['loc'] for _ in crawl_point[base_url]['urls']]

    def skip_check(self, url:str) -> bool:
        '''
        前回の続きの指定がある場合、前回のクロールポイントの５件のurlまで読み込みが完了しているか判定を行う。
        （完了→False、未完→True）
        前回の続きの指定がない場合、常にTrueを返す。
        ※前回のクロールポイントには10件のurlがあるが、url取得中に更新されトップページへ移動している可能性がある。
          無限ループに陥らないように5/10件で完了とさせる。
        '''
        if 'continued' in self.kwargs_save:
            if len(self.last_time_urls) <= 5:
                self.crwal_flg = False

            if url in self.last_time_urls:
                self.last_time_urls.remove(url)

        return self.crwal_flg

