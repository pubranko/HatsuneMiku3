from datetime import datetime
from shared.settings import TIMEZONE
from prefect_lib.flow.stats_analysis_report_flow import flow
from prefect_lib.data_models.stats_analysis_report_input import StatsAnalysisReportConst


flow.run(parameters=dict(
    report_term=StatsAnalysisReportConst.REPORT_TERM__DAILY,
    # report_term=StatsAnalysisReportConst.REPORT_TERM__WEEKLY,
    # report_term=StatsAnalysisReportConst.REPORT_TERM__MONTHLY,
    # report_term=StatsAnalysisReportConst.REPORT_TERM__YEARLY,
    totalling_term=StatsAnalysisReportConst.TOTALLING_TERM__DAILY,
    # totalling_term=StatsAnalysisReportConst.TOTALLING_TERM__WEEKLY,
    # totalling_term=StatsAnalysisReportConst.TOTALLING_TERM__MONTHLY,
    # totalling_term=StatsAnalysisReportConst.TOTALLING_TERM__YEARLY,
    base_date=datetime(2023, 3, 20).astimezone(TIMEZONE),   # 左記基準日の１日前、１週間前、１ヶ月前、１年前のデータが対象となる。
))

'''
レポート期間(report_term)と基準日(base_date)を基に基準期間(base_date_from, base_date_to)を取得する。
※基準日(base_date)=基準期間to(base_date_to)となる。
'''