from datetime import datetime
from common_lib.common_settings import TIMEZONE
from prefect_lib.flow.scrapy_crawling_flow import flow
# domain、crawling_start_time_*による絞り込みは任意
flow.run(parameters=dict(
    #domain='epochtimes.jp',    #'sankei.com',"jp.reuters.com","asahi.com","yomiuri.co.jp","kyodo.co.jp"
    #domain='yomiuri.co.jp',
    #domain='mainichi.jp',
    domain='nikkei.com',
    #crawling_start_time_from=datetime(2022, 2, 11, 16, 45, 0, 0).astimezone(TIMEZONE),
    crawling_start_time_from=datetime(2022, 6, 11, 11, 00, 0, 0).astimezone(TIMEZONE),
    #crawling_start_time_to=datetime(2021, 9, 25, 11, 8, 28, 286000).astimezone(TIMEZONE),
    #urls=['https://www.sankei.com/article/20210829-2QFVABFPMVIBNHSINK6TBYWEXE/?outputType=theme_tokyo2020',]
    following_processing_execution='Yes',    # 後続処理実行(news_clip_masterへの登録,solrへの登録)
))