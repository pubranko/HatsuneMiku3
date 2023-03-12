import os
import sys
from typing import Final
path = os.getcwd()
sys.path.append(path)
from prefect_lib.task.extentions_task import ExtensionsTask
from BrownieAtelierMongo.collection_models.controller_model import ControllerModel
from prefect.engine import state
from prefect.engine.runner import ENDRUN

class StopControllerUpdateTask(ExtensionsTask):
    '''
    ストップコントローラー更新用タスク
    '''
    # 定数定義
    REGISTER_ADD:Final[str] = 'add'         # 登録方法：追加
    REGISTER_DELETE:Final[str] = 'delete'   # 登録方法：削除

    def run(self, domain: str, register: str, destination: str):
        '''ここがprefectで起動するメイン処理'''
        self.run_init()

        self.logger.info('=== Stop Controller Update Task run kwargs : ' +
                    str(domain) + '/' + str(register) + '/' + str(destination))
        controller = ControllerModel(self.mongo)
        if destination == 'crawling':
            record: list = controller.crawling_stop_domain_list_get()
        elif destination == 'scrapying':
            record: list = controller.scrapying_stop_domain_list_get()
        else:
            self.logger.error(
                f'=== Stop Controller Update Task : run : destinationパラメータエラー : {destination}')
            raise ENDRUN(state=state.Failed())

        self.logger.info(
            f'=== Stop Controller Update Task : run : 更新前の登録状況 : {record}')

        if register == self.REGISTER_ADD:
            pass
            record.append(domain)
        elif register == self.REGISTER_DELETE:
            pass
            if domain in record:
                record.remove(domain)
            else:
                self.logger.error(
                    f'=== Stop Controller Update Task : run : domainの登録がありません : {domain}')
                raise ENDRUN(state=state.Failed())
        else:
            self.logger.error(
                f'=== Stop Controller Update Task : run : registerパラメータエラー : {register}')
            raise ENDRUN(state=state.Failed())

        # domainの重複除去
        _ = list(set(record))

        # 更新した内容でアップデート
        if destination == 'crawling':
            controller.crawling_stop_domain_list_update(_)
        else:
            controller.scrapying_stop_domain_list_update(_)

        self.logger.info(
            f'=== Stop Controller Update Task : run : 更新後の登録状況 : {_}')

        # 終了処理
        self.closed()
        # return ''
