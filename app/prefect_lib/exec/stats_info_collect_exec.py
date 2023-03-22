from datetime import datetime
from shared.settings import TIMEZONE
from prefect_lib.flow.stats_info_collect_flow import flow

'''基準日(0:00:00)〜翌日(0:00:00)までの期間が対象となる。'''
flow.run(parameters=dict(
    base_date=datetime(2023, 3, 19).astimezone(TIMEZONE),
))


