from prefect_lib.flow.stop_controller_update_flow import flow
from prefect_lib.task.stop_controller_update_task import StopControllerUpdateTask
# domain、crawling_start_time_*による絞り込みは任意
flow.run(parameters=dict(
    # domain='epochtimes.jp',
    # register='in',
    # destination='crawling',
    # domain='epochtimes.jp',
    # register='out',
    # destination='crawling',
    # domain='sankei.com',
    # register='in',
    # destination='scrapying',
    domain='sankei.com',
    # register=StopControllerUpdateTask.REGISTER_ADD,
    register=StopControllerUpdateTask.REGISTER_DELETE,
    # destination=StopControllerUpdateTask.CRAWLING,
    destination=StopControllerUpdateTask.SCRAPYING,
))
