import os
import sys
path = os.getcwd()
sys.path.append(path)
from prefect_lib.task.extentions_task import ExtensionsTask
from models.mongo_model import MongoModel
from models.controller_model import ControllerModel
from prefect.engine import state
from prefect.engine.runner import ENDRUN

class StopControllerUpdateTask(ExtensionsTask):
    '''
    ストップコントローラー更新用タスク
    '''

    def run(self, domain: str, in_out: str, destination: str):
        '''ここがprefectで起動するメイン処理'''
        self.run_init()

        self.logger.info('=== Stop Controller Update Task run kwargs : ' +
                    str(domain) + '/' + str(in_out) + '/' + str(destination))
        mongo: MongoModel = self.mongo
        controller = ControllerModel(mongo)
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

        if in_out == 'in':
            pass
            record.append(domain)
        elif in_out == 'out':
            pass
            if domain in record:
                record.remove(domain)
            else:
                self.logger.error(
                    f'=== Stop Controller Update Task : run : domainの登録がありません : {domain}')
                raise ENDRUN(state=state.Failed())
        else:
            self.logger.error(
                f'=== Stop Controller Update Task : run : in_outパラメータエラー : {in_out}')
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
