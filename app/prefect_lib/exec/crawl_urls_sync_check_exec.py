from prefect_lib.flow.crawl_urls_sync_check_flow import flow
# domain、start_time_*による絞り込みは任意
flow.run(parameters=dict(
    #domain='sankei.com',
    #start_time_from=datetime(2021, 9, 25, 11, 8, 28, 286000).astimezone(TIMEZONE),
    #start_time_to=datetime(2021, 9, 25, 11, 8, 28, 286000).astimezone(TIMEZONE),
    #urls=['https://www.sankei.com/article/20210829-2QFVABFPMVIBNHSINK6TBYWEXE/?outputType=theme_tokyo2020',]
))
#2021-08-29T13:33:48.503Z