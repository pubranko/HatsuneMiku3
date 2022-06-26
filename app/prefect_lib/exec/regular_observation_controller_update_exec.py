from prefect_lib.flow.regular_observation_controller_update_flow import flow
# domain、crawling_start_time_*による絞り込みは任意
flow.run(parameters=dict(
    in_out='in',
    spiders_name=[
        'asahi_com_sitemap',
        'jp_reuters_com_crawl',
        'kyodo_co_jp_sitemap',
        'sankei_com_sitemap',
        'yomiuri_co_jp_sitemap',
        'epochtimes_jp_crawl',
        #'epochtimes_jp_sitemap',
    ],
))
