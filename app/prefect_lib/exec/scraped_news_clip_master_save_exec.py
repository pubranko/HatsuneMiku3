from datetime import datetime
from shared.settings import TIMEZONE
from prefect_lib.flow.scraped_news_clip_master_save_flow import flow

# domain、scrapying_start_time_*による絞り込みは任意
flow.run(parameters=dict(
    #domain='kyodo.co.jp',
    #domain='epochtimes.jp',
    target_start_time_from=datetime(2023, 3, 16, 0, 0, 0).astimezone(TIMEZONE),
    #target_start_time_to=datetime(2021, 8, 21, 10, 18, 12, 160000).astimezone(TIMEZONE),
    #target_start_time_from=datetime(2021, 9, 25, 15, 26, 37, 344148).astimezone(TIMEZONE),
    #target_start_time_to=datetime(2021, 9, 25, 15, 26, 37, 344148).astimezone(TIMEZONE),
))