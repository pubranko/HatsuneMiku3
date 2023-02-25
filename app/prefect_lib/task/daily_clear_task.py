import os
import sys
from prefect.engine import state
from prefect.engine.runner import ENDRUN
path = os.getcwd()
sys.path.append(path)
from prefect_lib.task.extentions_task import ExtensionsTask
from BrownieAtelierMongo.models.scraped_from_response_model import ScrapedFromResponseModel


class DailyClearTask(ExtensionsTask):
    '''
    '''

    def run(self, **kwargs):
        ''''''
        def delete_non_filter(collection_name: str, collection: ScrapedFromResponseModel,) -> None:
            delete_count: int = collection.delete_many(filter={})
            self.logger.info(f'=== DailyClearTask run delete_non_filter : 削除件数({collection_name}) : {delete_count}件')

        ###################################################################################
        self.run_init()

        collections_name: list = ['scraped_from_response']

        for collection_name in collections_name:
            collection = ScrapedFromResponseModel(self.mongo)
            # データ全削除
            delete_non_filter(
                collection_name, collection)

        # 終了処理
        self.closed()
        # return ''
