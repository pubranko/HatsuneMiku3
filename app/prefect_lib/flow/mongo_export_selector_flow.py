import os
import sys
from datetime import datetime
from prefect.core.flow import Flow
from prefect.core.parameter import Parameter
from prefect.tasks.control_flow.conditional import ifelse
from prefect.engine import signals
from prefect.utilities.context import Context
path = os.getcwd()
sys.path.append(path)
from prefect_lib.common_module.flow_status_change import flow_status_change
from prefect_lib.task.mongo_export_selector_task import MongoExportSelectorTask
from BrownieAtelierMongo.collection_models.crawler_response_model import CrawlerResponseModel
from BrownieAtelierMongo.collection_models.news_clip_master_model import NewsClipMasterModel
from BrownieAtelierMongo.collection_models.crawler_logs_model import CrawlerLogsModel
from BrownieAtelierMongo.collection_models.asynchronous_report_model import AsynchronousReportModel
from BrownieAtelierMongo.collection_models.controller_model import ControllerModel


'''
mongoDBのエクスポートを行う。
・mongoDBから取得したコレクションをpythonのlistにし、pickle.dumpsでシリアライズしてエクスポートする。
・対象のコレクションを選択できる。
・対象の年月を指定できる。範囲を指定した場合、月ごとにエクスポートを行う。
・crawler_responseの場合、登録済みになったレコードのみとするか全てとするか選択できる。
'''
with Flow(
    name='[MONGO_003] Mongo export selector flow',
    state_handlers=[flow_status_change],
) as flow:
    collections_name = Parameter(
        'collections_name', required=True,
        default=[
            CrawlerResponseModel.COLLECTION_NAME,
            NewsClipMasterModel.COLLECTION_NAME,
            CrawlerLogsModel.COLLECTION_NAME,
            AsynchronousReportModel.COLLECTION_NAME,
            ControllerModel.COLLECTION_NAME,
        ])
    prefix = Parameter('prefix', required=False,)

    export_period_from = Parameter('export_period_from', required=False,)
    export_period_to = Parameter('export_period_to', required=False,)

    crawler_response__registered = Parameter(
        'crawler_response__registered', required=False,)
    task = MongoExportSelectorTask()
    result = task(collections_name=collections_name,
                  export_period_from=export_period_from,
                  export_period_to=export_period_to,
                  prefix=prefix,
                  crawler_response__registered=crawler_response__registered,
                  )
