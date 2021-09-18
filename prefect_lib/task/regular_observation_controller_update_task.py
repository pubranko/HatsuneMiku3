import os
import sys
import re
from logging import Logger
path = os.getcwd()
sys.path.append(path)
from prefect_lib.task.extentions_task import ExtensionsTask
from models.mongo_model import MongoModel
from models.controller_model import ControllerModel
from prefect.engine import state
from prefect.engine.runner import ENDRUN
from common_lib.directory_search import directory_search


class RegularObservationControllerUpdateTask(ExtensionsTask):
    '''
    定点観測コントローラー更新用タスク
    '''

    def run(self, spiders_name: str, in_out: str,):
        '''ここがprefectで起動するメイン処理'''

        logger: Logger = self.logger
        logger.info('=== Regular Observation Controller Update Task run kwargs : ' +
                    str(spiders_name) + '/' + str(in_out))
        mongo: MongoModel = self.mongo
        controller = ControllerModel(mongo)
        record = set(controller.regular_observation_spider_name_set_get())
        logger.info(
            '=== Stop Controller Update Task : run : 更新前の登録内容 : ' + str(record))

        # 空白除去しカンマ区切りのセットへ変換
        ptn = re.compile(r'\s|　')
        _ = ptn.sub('',spiders_name)
        spiders_name_set = set(_.split(','))

        # 存在するスパイダーのリスト生成
        spiders_info:list = directory_search()
        spiders_exist_set:set = set()
        for spider_info in spiders_info:
            spiders_exist_set.add(spider_info['spider_name'])

        if in_out == 'in':
            for spider_name in spiders_name_set:
                if not spider_name in spiders_exist_set:
                    logger.error(
                        '=== Stop Controller Update Task : run : spider_nameパラメータエラー : ' + spider_name + ' は存在しません。')
                    raise ENDRUN(state=state.Failed())
            record.update(spiders_name_set)
        elif in_out == 'out':
            for spider_name in spiders_name_set:
                if spider_name in record:
                    record.remove(spider_name)
                else:
                    logger.error(
                        '=== Stop Controller Update Task : run : spider_nameパラメータエラー : ' + spider_name + ' は登録されていません。')
                    raise ENDRUN(state=state.Failed())
        else:
            logger.error(
                '=== Stop Controller Update Task : run : in_outパラメータエラー : ')
            raise ENDRUN(state=state.Failed())

        logger.info(
            '=== Stop Controller Update Task : run : 更新後の登録内容 : ' + str(record))

        controller.regular_observation_spider_name_set_update(record)

        # 終了処理
        self.closed()
        # return ''
