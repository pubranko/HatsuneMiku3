import os
import sys
import logging
from logging import Logger
from typing import Any
from importlib import import_module
path = os.getcwd()
sys.path.append(path)
from prefect_lib.common_module.extentions_task import ExtensionsTask


class ScrapyingTask(ExtensionsTask):
    '''
    スクレイピング用タスク
    '''
    def run(self, **kwargs):
        '''ここがprefectで起動するメイン処理'''

        mod: Any = import_module(kwargs['module'])
        method = kwargs['method']
        kwargs['starting_time'] = self.starting_time
        kwargs['mongo'] = self.mongo

        logger: Logger = self.logger
        logger.info('=== ScrapyingTask run kwargs : ' + str(kwargs))
        getattr(mod, method)(kwargs)

        # 上記の結果をスクレイピング
        '''
        1. 自動スクレイピングモードを取得
        2. 自動スクレイピングモードonの場合、
           MongoDBより前回最後のレスポンスタイムを取得
        3. 前回最後のレスポンスタイムよりあとに発生しているレスポンスを取得する。
        4. a
        '''

        # 終了処理
        self.closed()
        # return ''