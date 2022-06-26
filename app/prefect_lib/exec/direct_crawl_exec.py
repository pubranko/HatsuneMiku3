from prefect_lib.flow.direct_crawl_flow import flow
# scraped_save_start_time_*による絞り込みは任意
flow.run(parameters=dict(
    spider_name='sankei_com_sitemap',
    file='sankei_com(test).txt',
    #spider_name='yomiuri_co_jp_sitemap',
    #file='yomiuri_co_jp(test).txt',
    #spider_name='asahi_com_sitemap',
    #file='asahi_com(test).txt',
    # domain='',
    #scrapying_start_time_from=datetime(2021, 8, 21, 0, 0, 0).astimezone(TIMEZONE),
    #scrapying_start_time_to=datetime(2021, 8, 21, 10, 18, 12, 160000).astimezone(TIMEZONE),
    #scrapying_start_time_from=datetime(2021, 8, 21, 10, 18, 12, 161000).astimezone(TIMEZONE),
    #scrapying_start_time_to=datetime(2021, 8, 21, 10, 18, 12, 160000).astimezone(TIMEZONE),
))
