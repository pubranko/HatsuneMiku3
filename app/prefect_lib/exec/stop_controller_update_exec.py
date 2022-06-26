from prefect_lib.flow.stop_controller_update_flow import flow
# domain、crawling_start_time_*による絞り込みは任意
flow.run(parameters=dict(
    # domain='epochtimes.jp',
    # in_out='in',
    # destination='crawling',
    # domain='epochtimes.jp',
    # in_out='out',
    # destination='crawling',
    # domain='sankei.com',
    # in_out='in',
    # destination='scrapying',
    domain='sankei.com',
    in_out='out',
    destination='crawling',
))
