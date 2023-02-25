from prefect_lib.flow.regular_observation_controller_update_flow import flow
from prefect_lib.task.regular_observation_controller_update_task import RegularObservationControllerUpdateTask
# domain、crawling_start_time_*による絞り込みは任意
flow.run(parameters=dict(
    register = RegularObservationControllerUpdateTask.REGISTER_ADD,
    # register = RegularObservationControllerUpdateTask.REGISTER_DELETE,
    spiders_name=[
        'asahi_com_sitemap',
        'epochtimes_jp_crawl',
        'jp_reuters_com_crawl',
        'kyodo_co_jp_sitemap',
        'mainichi_jp_crawl',
        'nikkei_com_crawl',
        'sankei_com_sitemap',
        'yomiuri_co_jp_sitemap',
    ],
))
