from prefect.run_configs import LocalRun
from prefect.run_configs import KubernetesRun
from prefect.storage import Docker
from datetime import datetime, timezone, timedelta
from prefect.storage import Local

# crawl scrape系
from prefect_lib.flow.direct_crawl_flow import flow as direct_crawl_flow
from prefect_lib.flow.first_observation_flow import flow as first_observation_flow
from prefect_lib.flow.regular_observation_flow import flow as regular_observation_flow
from prefect_lib.flow.scraped_news_clip_master_save_flow import flow as scraped_news_clip_master_save_flow
from prefect_lib.flow.scrapy_crawling_flow import flow as scrapy_crawling_flow
from prefect_lib.flow.scrapying_flow import flow as scrapying_flow
from prefect_lib.flow.solr_news_clip_save_flow import flow as solr_news_clip_save_flow
# # check系
from prefect_lib.flow.crawl_urls_sync_check_flow import flow as crawl_urls_sync_check_flow
# # mongodbメンテナンス消し
from prefect_lib.flow.daily_clear_flow import flow as daily_clear_flow
from prefect_lib.flow.monthly_delete_flow import flow as monthly_delete_flow
from prefect_lib.flow.mongo_export_selector_flow import flow as mongo_export_selector_flow
from prefect_lib.flow.mongo_import_selector_flow import flow as mongo_import_selector_flow
# # ENTRY系
from prefect_lib.flow.scraper_info_uploader_flow import flow as scraper_info_uploader_flow
from prefect_lib.flow.stop_controller_update_flow import flow as stop_controller_update_flow
from prefect_lib.flow.regular_observation_controller_update_flow import flow as regular_observation_controller_update_flow
# # stats系
from prefect_lib.flow.scraper_pattern_report_flow import flow as scraper_pattern_report_flow
from prefect_lib.flow.stats_analysis_report_flow import flow as stats_analysis_report_flow
from prefect_lib.flow.stats_info_collect_flow import flow as stats_info_collect_flow

# 各種共通設定
# エージェント名
agents: list = ['crawler-container']
# プロジェクト名
project_name: str = "TEST2"
# 登録したフローのファイル名に付与するタイムスタンプ
dt_now_jst = datetime.now(timezone(timedelta(hours=9)))
# timestamp = dt_now_jst.isoformat().replace(":","-")
timestamp = dt_now_jst.strftime('%Y-%m-%dT%H-%M-%S.%f')


LocalRun()
# crawl scrape系
# direct_crawl_flow.register(
#     labels=agents, project_name=project_name, add_default_labels=False, path=f'.flows/{direct_crawl_flow.name}/{timestamp}')
# first_observation_flow.register(
#     labels=agents, project_name=project_name, add_default_labels=False, path=f'.flows/{first_observation_flow.name}/{timestamp}')
# regular_observation_flow.register(
#     labels=agents, project_name=project_name, add_default_labels=False, path=f'.flows/{regular_observation_flow.name}/{timestamp}')
# scraped_news_clip_master_save_flow.register(
#     labels=agents, project_name=project_name, add_default_labels=False, path=f'.flows/{scraped_news_clip_master_save_flow.name}/{timestamp}')
# scrapy_crawling_flow.register(
#     labels=agents, project_name=project_name, add_default_labels=False, path=f'.flows/{scrapy_crawling_flow.name}/{timestamp}')
# scrapying_flow.register(
#     labels=agents, project_name=project_name, add_default_labels=False, path=f'.flows/{scrapying_flow.name}/{timestamp}')
# solr_news_clip_save_flow.register(
#     labels=agents, project_name=project_name, add_default_labels=False, path=f'.flows/{solr_news_clip_save_flow.name}/{timestamp}')
# check系
# crawl_urls_sync_check_flow.register(
#     labels=agents, project_name=project_name, add_default_labels=False, path=f'.flows/{crawl_urls_sync_check_flow.name}/{timestamp}')
# mongodbメンテナンス消し
# daily_clear_flow.register(
#     labels=agents, project_name=project_name, add_default_labels=False, path=f'.flows/{daily_clear_flow.name}/{timestamp}')
# monthly_delete_flow.register(
#     labels=agents, project_name=project_name, add_default_labels=False, path=f'.flows/{monthly_delete_flow.name}/{timestamp}')
# mongo_export_selector_flow.register(
#     labels=agents, project_name=project_name, add_default_labels=False, path=f'.flows/{mongo_export_selector_flow.name}/{timestamp}')
# mongo_import_selector_flow.register(
#     labels=agents, project_name=project_name, add_default_labels=False, path=f'.flows/{mongo_import_selector_flow.name}/{timestamp}')
# ENTRY系
# scraper_info_uploader_flow.register(
#     labels=agents, project_name=project_name, add_default_labels=False, path=f'.flows/{scraper_info_uploader_flow.name}/{timestamp}')
stop_controller_update_flow.storage = Local(
    # directory='.flows/',
    path='prefect_lib/exec/stop_controller_update_exec.py',
    add_default_labels=False,
    stored_as_script=True)
stop_controller_update_flow.register(
    labels=agents, project_name=project_name, add_default_labels=False,
    # path='/app/prefect_lib/flow/stop_controller_update_flow.py',
    #path='prefect_lib/exec/stop_controller_update_exec.py',
    # files={'prefect_lib/flow/stop_controller_update_flow.py':'stop_controller_update_flow.py'}
    )
    # labels=agents, project_name=project_name, add_default_labels=False, path=f'.flows/{stop_controller_update_flow.name}/{timestamp}')
# regular_observation_controller_update_flow.register(
#     labels=agents, project_name=project_name, add_default_labels=False, path=f'.flows/{regular_observation_controller_update_flow.name}/{timestamp}')
# stats系
# scraper_pattern_report_flow.register(
#     labels=agents, project_name=project_name, add_default_labels=False, path=f'.flows/{scraper_pattern_report_flow.name}/{timestamp}')
# stats_analysis_report_flow.register(
#     labels=agents, project_name=project_name, add_default_labels=False, path=f'.flows/{stats_analysis_report_flow.name}/{timestamp}')
# stats_info_collect_flow.register(
#     labels=agents, project_name=project_name, add_default_labels=False, path=f'.flows/{stats_info_collect_flow.name}/{timestamp}')
