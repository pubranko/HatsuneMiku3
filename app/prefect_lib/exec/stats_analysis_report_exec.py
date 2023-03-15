from datetime import datetime
from shared.settings import TIMEZONE
from prefect_lib.flow.stats_analysis_report_flow import flow
from prefect_lib.data_models.stats_analysis_report_input import StatsAnalysisReportInput

const = StatsAnalysisReportInput()

flow.run(parameters=dict(
    #report_term=const.REPORT_TERM__DAILY,
    #report_term=const.REPORT_TERM__WEEKLY,
    #report_term=const.REPORT_TERM__MONTHLY,
    #report_term=const.REPORT_TERM__YEARLY,
    totalling_term=const.TOTALLING_TERM__DAILY,
    # totalling_term=const.TOTALLING_TERM__WEEKLY,
    # totalling_term=const.TOTALLING_TERM__MONTHLY,
    # totalling_term=const.TOTALLING_TERM__YEARLY,
    base_date=datetime(2022, 6, 26).astimezone(TIMEZONE),   # 左記基準日の前日分のデータが対象となる。
))
