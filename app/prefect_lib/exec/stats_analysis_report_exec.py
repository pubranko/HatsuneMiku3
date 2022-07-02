from datetime import datetime
from common_lib.common_settings import TIMEZONE
from prefect_lib.flow.stats_analysis_report_flow import flow
flow.run(parameters=dict(
    #report_term='daily',
    report_term='weekly',
    #report_term='monthly',
    #report_term='yearly',
    totalling_term='daily',
    base_date=datetime(2022, 6, 26).astimezone(TIMEZONE),   # 左記基準日の前日分のデータが対象となる。
))
