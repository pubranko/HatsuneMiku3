import os
import sys
from typing import Any, Optional
from datetime import datetime
from logging import LoggerAdapter
from scrapy.exceptions import CloseSpider
path = os.getcwd()
sys.path.append(path)
from shared.timezone_recovery import timezone_recovery
from BrownieAtelierMongo.collection_models.controller_model import ControllerModel


class LastmodContinuedSkipCheck(object):
    '''
    前回の続きからクロールに関する機能を
    '''
    # 引数保存エリア
    continued: Optional[bool]     # spider起動時の引数の値
    spider_name: str
    domain_name: str
    controller:ControllerModel
    logger: LoggerAdapter
    # クロールポイントレコード情報保存
    crawl_point: dict = {}
    latest_lastmod: Any = None
    latest_urls: Any = None

    def __init__(self,
        continued: Optional[bool],
        spider_name: str,
        domain_name: str,
        controller: ControllerModel,
        logger: LoggerAdapter,
        ) -> None:
        '''
        '''
        # 引数保存
        self.continued = continued  # クラス変数に保存
        self.spider_name = spider_name
        self.domain_name = domain_name
        self.controller = controller # コントローラーコレクションを保存
        self.logger = logger

        # 前回からの続き指定がある場合
        if self.continued:
            # コントローラーから前回のクロールポイントを取得
            self.crawl_point = self.controller.crawl_point_get(
                domain_name, spider_name)

            # 前回のクロールポイントが空の場合、エラー処理を実施
            if not self.crawl_point:
                self.logger.critical(
                    f'引数エラー：domain = {domain_name} は前回のクロール情報がありません。初回から"continued"の使用は不可です。')
                raise CloseSpider()

            # lastmodがあるサイトの場合、タイムゾーンがmongoDBから取得する際消えているため復元する。
            if self.controller.LATEST_LASTMOD in self.crawl_point:
                self.latest_lastmod = timezone_recovery(
                    self.crawl_point[self.controller.LATEST_LASTMOD])


    def skip_check(self, lastmod: datetime) -> bool:
        '''
        前回の続きの指定がある場合、前回のクロール時点のlastmod（最終更新日時）と比較を行う。
        ・前回のクロール時点lastmodより新しい場合、スキップ対象外（False）とする。
        ・上記以外の場合、引数のurlをスキップ対象（True）とする。
        ・ただし、前回の続きの指定がない場合、常にスキップ対象外（False）を返す。
        '''
        skip_flg: bool = False
        if self.continued:
            if lastmod < self.latest_lastmod:
                skip_flg = True
        return skip_flg

    def max_lastmod_dicision(self, in_the_page_max_lastmod: datetime) -> datetime:
        '''
        サイトマップ内の最大更新時間を引数として受け取る。
        受け取った引数とドメイン内の最大更新時間の記録と比較し、
        新しい最大更新時間を決定する。
        '''
        if self.latest_lastmod:
            if in_the_page_max_lastmod > self.latest_lastmod:
                return in_the_page_max_lastmod
            else:
                return self.latest_lastmod
        else:
            self.latest_lastmod = in_the_page_max_lastmod
            return in_the_page_max_lastmod
