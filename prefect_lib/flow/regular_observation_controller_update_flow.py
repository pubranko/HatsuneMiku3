import os
import sys
from datetime import datetime
from prefect.core.flow import Flow
from prefect.core.parameter import Parameter
from prefect.tasks.control_flow.conditional import ifelse
from prefect.utilities.context import Context
path = os.getcwd()
sys.path.append(path)
from prefect_lib.settings import TIMEZONE
from prefect_lib.common_module.logging_setting import LOG_FILE_PATH
from prefect_lib.common_module.flow_status_change import flow_status_change
from prefect_lib.task.regular_observation_controller_update_task import RegularObservationControllerUpdateTask

'''
定期観測対象のスパイダーの登録・削除を行う。
'''
with Flow(
    name='Regular observation controller update Flow',
    state_handlers=[flow_status_change],
) as flow:

    spiders_name = Parameter(
        'spiders_name', required=True)()  # 登録・削除したいドメインを指定
    in_out = Parameter('in_out', required=True)()  # in:登録、out：削除

    task = RegularObservationControllerUpdateTask(
        log_file_path=LOG_FILE_PATH, start_time=datetime.now().astimezone(TIMEZONE))
    result = task(spiders_name=spiders_name, in_out=in_out)

# domain、crawling_start_time_*による絞り込みは任意
flow.run(parameters=dict(
    in_out='in',
    spiders_name='asahi_com_sitemap,epochtimes_jp_sitemap,jp_reuters_com_crawl,kyodo_co_jp_sitemap,sankei_com_sitemap,yomiuri_co_jp_sitemap',
    #spiders_name='jp_reuters_com_crawl,kyodo_co_jp_sitemap,yomiuri_co_jp_sitemap',
    #spiders_name='asahi_com_sitemap, epochtimes_jp_sitemap,sankei_com_sitemap',
))
