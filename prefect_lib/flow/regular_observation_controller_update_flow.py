import os
import sys
import logging
from datetime import datetime
from prefect import Flow, task, Parameter
from prefect.tasks.control_flow.conditional import ifelse
from prefect.utilities.context import Context
path = os.getcwd()
sys.path.append(path)
from prefect_lib.task.regular_observation_controller_update_task import RegularObservationControllerUpdateTask
from prefect_lib.settings import TIMEZONE
from prefect_lib.common_module.flow_status_change import flow_status_change

start_time = datetime.now().astimezone(
    TIMEZONE)
log_file_path = os.path.join(
    'logs', os.path.splitext(os.path.basename(__file__))[0] + '.log')
logging.basicConfig(level=logging.DEBUG, filemode="w+", filename=log_file_path,
                    format='%(asctime)s %(levelname)s [%(name)s] : %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

with Flow(
    name='Regular observation controller update Flow',
    state_handlers=[flow_status_change],
) as flow:

    spiders_name = Parameter(
        'spiders_name', required=True)()  # 登録・削除したいドメインを指定
    in_out = Parameter('in_out', required=True)()  # in:登録、out：削除

    task = RegularObservationControllerUpdateTask(
        log_file_path=log_file_path, start_time=start_time)
    result = task(spiders_name=spiders_name, in_out=in_out)

# domain、crawling_start_time_*による絞り込みは任意
flow.run(parameters=dict(
    in_out='in',
    # spiders_name='asahi_com_sitemap,epochtimes_jp_sitemap,jp_reuters_com_crawl,kyodo_co_jp_sitemap,sankei_com_sitemap,yomiuri_co_jp_sitemap',
    spiders_name='jp_reuters_com_crawl,kyodo_co_jp_sitemap,yomiuri_co_jp_sitemap',
    #spiders_name='asahi_com_sitemap, epochtimes_jp_sitemap,sankei_com_sitemap',
))
