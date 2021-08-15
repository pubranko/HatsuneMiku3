import os
import sys
import logging
from logging import Logger
from typing import Any
from importlib import import_module
path = os.getcwd()
sys.path.append(path)
from prefect_lib.common_module.extentions_task import ExtensionsTask
from models.mongo_model import MongoModel
from models.crawler_response_model import CrawlerResponseModel


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
        mongo: MongoModel = kwargs['mongo']
        kwargs['crawler_response'] = CrawlerResponseModel(mongo)

        logger: Logger = self.logger
        logger.info('=== ScrapyingTask run kwargs : ' + str(kwargs))
        getattr(mod, method)(kwargs)


        # 終了処理
        self.closed()
        # return ''