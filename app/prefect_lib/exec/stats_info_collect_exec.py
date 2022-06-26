from datetime import datetime
from prefect_lib.settings import TIMEZONE
from prefect_lib.flow.stats_info_collect_flow import flow

flow.run(parameters=dict(
    base_date=datetime(2022, 6, 25).astimezone(TIMEZONE),
))
