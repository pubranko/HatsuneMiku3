import os
import sys
from typing import Final
path = os.getcwd()
sys.path.append(path)
from prefect_lib.task.extentions_task import ExtensionsTask
from BrownieAtelierMongo.models.mongo_model import MongoModel
from BrownieAtelierMongo.models.controller_model import ControllerModel
from prefect.engine import state
from prefect.engine.runner import ENDRUN
from shared.directory_search_spiders import DirectorySearchSpiders


class RegularObservationControllerUpdateTask(ExtensionsTask):
    '''
    定期観測コントローラー更新用タスク
    '''
    # 定数定義
    REGISTER_ADD:Final[str] = 'add'         # 登録方法：追加
    REGISTER_DELETE:Final[str] = 'delete'   # 登録方法：削除

    def run(self, spiders_name: str, register: str,):
        '''ここがprefectで起動するメイン処理'''
        self.run_init()

        self.logger.info('=== Regular Observation Controller Update Task run kwargs : ' +
                    str(spiders_name) + '/' + str(register))
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

        if register == self.REGISTER_ADD:
            for spider_name in spiders_name_set:
                if not spider_name in spiders_exist_set:
                    self.logger.error(
                        '=== Regular Observation  Controller Update Task : run : spider_nameパラメータエラー : ' + spider_name + ' は存在しません。')
                    raise ENDRUN(state=state.Failed())
            record.update(spiders_name_set)
        elif register == self.REGISTER_DELETE:
            for spider_name in spiders_name_set:
                if spider_name in record:
                    record.remove(spider_name)
                else:
                    self.logger.error(
                        '=== Regular Observation  Controller Update Task : run : spider_nameパラメータエラー : ' + spider_name + ' は登録されていません。')
                    raise ENDRUN(state=state.Failed())
        else:
            self.logger.error(
                '=== Regular Observation  Controller Update Task : run : 登録方法(register)パラメータエラー : ')
            raise ENDRUN(state=state.Failed())

        self.logger.info(
            '=== Regular Observation  Controller Update Task : run : 更新後の登録内容 : ' + str(record))

        controller.regular_observation_spider_name_set_update(record)

        # 終了処理
        self.closed()
        # return ''
