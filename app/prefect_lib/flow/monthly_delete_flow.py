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
from prefect_lib.task.monthly_delete_task import MonthlyDeleteTask
from BrownieAtelierMongo.collection_models.crawler_logs_model import CrawlerLogsModel
from BrownieAtelierMongo.collection_models.crawler_response_model import CrawlerResponseModel
from BrownieAtelierMongo.collection_models.asynchronous_report_model import AsynchronousReportModel


'''
月次削除処理
・コレクションのデータを年月単位で削除を行う。
・コレクションを選択可能。
・範囲指定もできる。
'''
with Flow(
    name='[MONGO_002] Monthly delete flow',
    state_handlers=[flow_status_change],
) as flow:
    collections_name = Parameter(
        'collections_name', required=True,
        default=[
            CrawlerResponseModel.COLLECTION_NAME,
            CrawlerLogsModel.COLLECTION_NAME,
            AsynchronousReportModel.COLLECTION_NAME,
        ])
    delete_period_from = Parameter('delete_period_from', required=False,)
    delete_period_to = Parameter('delete_period_to', required=False,)
    task = MonthlyDeleteTask()
    result = task(collections_name=collections_name,
        delete_period_from=delete_period_from,
        delete_period_to=delete_period_to,
        )
