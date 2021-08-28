# pylint: disable=E1101
import os
import sys
from typing import Any
from importlib import import_module
path = os.getcwd()
sys.path.append(path)
from prefect_lib.task.extentions_task import ExtensionsTask

class ScrapyCrawlingTask(ExtensionsTask):
    '''
    定期観測用タスク
    '''
    def run(self,module,method):
        '''ここがprefectで起動するメイン処理'''
        mod:Any = import_module(module)
        getattr(mod,method)(self.starting_time)
        # 終了処理
        self.closed()
        #return ''
