import os
import sys
from datetime import datetime
# from prefect import Flow
# from prefect import Flow, task, Parameter
from prefect.core.flow import Flow
from prefect.core.parameter import Parameter
from prefect.core.parameter import DateTimeParameter
from prefect.tasks.control_flow.conditional import ifelse
from prefect.engine import signals
from prefect.utilities.context import Context
path = os.getcwd()
sys.path.append(path)
from prefect_lib.settings import TIMEZONE
from prefect_lib.common_module.logging_setting import LOG_FILE_PATH
from prefect_lib.common_module.flow_status_change import flow_status_change
from prefect_lib.task.stats_analysis_report_task import StatsAnalysisReportTask

'''
各種実行結果を解析しレポートとして出力する。
'''
with Flow(
    name='[STATS_002] Stats nalysis report flow',
    state_handlers=[flow_status_change],
) as flow:
    report_term = Parameter('report_term', default='weekly', required=True)()   # レポート期間 : daily, weekly, monthly, yearly
    totalling_term = Parameter('totalling_term', default='daily', required=True)()   # レポート期間 : daily, weekly, monthly, yearly
    base_date = DateTimeParameter('base_date', required=False,)
    task = StatsAnalysisReportTask(
        log_file_path=LOG_FILE_PATH, start_time=datetime.now().astimezone(TIMEZONE))
    result = task(report_term=report_term,
                  totalling_term=totalling_term,
                  base_date=base_date,)
