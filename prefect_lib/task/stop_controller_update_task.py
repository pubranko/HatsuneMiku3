import os
import sys
from logging import Logger
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

        logger: Logger = self.logger
        logger.info('=== Stop Controller Update Task run kwargs : ' +
                    str(domain) + '/' + str(in_out) + '/' + str(destination))
        mongo: MongoModel = self.mongo
        controller = ControllerModel(mongo)
        if destination == 'crawling':
            record: list = controller.crawling_stop_domain_list_get()
        elif destination == 'scrapying':
            record: list = controller.scrapying_stop_domain_list_get()
        else:
            logger.error(
                '=== Stop Controller Update Task : run : destinationパラメータエラー : ')
            raise ENDRUN(state=state.Failed())

        if in_out == 'in':
            pass
            record.append(domain)
        elif in_out == 'out':
            pass
            if domain in record:
                record.remove(domain)
            else:
                logger.error(
                    '=== Stop Controller Update Task : run : domainパラメータエラー : ')
                raise ENDRUN(state=state.Failed())
        else:
            logger.error(
                '=== Stop Controller Update Task : run : in_outパラメータエラー : ')
            raise ENDRUN(state=state.Failed())

        if destination == 'crawling':
            controller.crawling_stop_domain_list_update(record)
        else:
            controller.scrapying_stop_domain_list_update(record)

        # 終了処理
        self.closed()
        # return ''
