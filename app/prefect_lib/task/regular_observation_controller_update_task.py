import os
import sys
import re
path = os.getcwd()
sys.path.append(path)
from prefect_lib.task.extentions_task import ExtensionsTask
from models.mongo_model import MongoModel
from models.controller_model import ControllerModel
from prefect.engine import state
from prefect.engine.runner import ENDRUN
from common_lib.directory_search_spiders import DirectorySearchSpiders


class RegularObservationControllerUpdateTask(ExtensionsTask):
    '''
    定期観測コントローラー更新用タスク
    '''

    def run(self, spiders_name: str, in_out: str,):
        '''ここがprefectで起動するメイン処理'''
        self.run_init()

        self.logger.info('=== Regular Observation Controller Update Task run kwargs : ' +
                    str(spiders_name) + '/' + str(in_out))
        mongo: MongoModel = self.mongo
        controller = ControllerModel(mongo)
        record = set(controller.regular_observation_spider_name_set_get())
        self.logger.info(
            '=== Regular Observation  Controller Update Task : run : 更新前の登録内容 : ' + str(record))

        # 引数のスパイダー情報リストをセットへ変換（重複削除）
        spiders_name_set = set(spiders_name)

        # 存在するスパイダーのリスト生成
        directory_search_spiders = DirectorySearchSpiders()
        spiders_exist_set:set = set()
        for spider_info in directory_search_spiders.spiders_name_list_get():
            spiders_exist_set.add(spider_info)

        if in_out == 'in':
            for spider_name in spiders_name_set:
                if not spider_name in spiders_exist_set:
                    self.logger.error(
                        '=== Regular Observation  Controller Update Task : run : spider_nameパラメータエラー : ' + spider_name + ' は存在しません。')
                    raise ENDRUN(state=state.Failed())
            record.update(spiders_name_set)
        elif in_out == 'out':
            for spider_name in spiders_name_set:
                if spider_name in record:
                    record.remove(spider_name)
                else:
                    self.logger.error(
                        '=== Regular Observation  Controller Update Task : run : spider_nameパラメータエラー : ' + spider_name + ' は登録されていません。')
                    raise ENDRUN(state=state.Failed())
        else:
            self.logger.error(
                '=== Regular Observation  Controller Update Task : run : in_outパラメータエラー : ')
            raise ENDRUN(state=state.Failed())

        self.logger.info(
            '=== Regular Observation  Controller Update Task : run : 更新後の登録内容 : ' + str(record))

        controller.regular_observation_spider_name_set_update(record)

        # 終了処理
        self.closed()
        # return ''
