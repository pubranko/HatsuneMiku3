# crawl_scrape
import os

# if os.getenv('PATH'):

agents:list = ['crawler-container']
project_name:str = os.environ['PRECECT_PROJECT']

# crawl scrape系
from prefect_lib.flow.direct_crawl_flow import flow
flow.register(labels=agents ,project_name=project_name)
from prefect_lib.flow.first_observation_flow import flow
flow.register(labels=agents ,project_name=project_name)
from prefect_lib.flow.regular_observation_flow import flow
flow.register(labels=agents ,project_name=project_name)
from prefect_lib.flow.scraped_news_clip_master_save_flow import flow
flow.register(labels=agents ,project_name=project_name)
from prefect_lib.flow.scrapy_crawling_flow import flow
flow.register(labels=agents ,project_name=project_name)
from prefect_lib.flow.scrapying_flow import flow
flow.register(labels=agents ,project_name=project_name)
from prefect_lib.flow.solr_news_clip_save_flow import flow
flow.register(labels=agents ,project_name=project_name)

# check系
from prefect_lib.flow.crawl_urls_sync_check_flow import flow
flow.register(labels=agents ,project_name=project_name)

# mongodbメンテナンス消し
from prefect_lib.flow.daily_clear_flow import flow
flow.register(labels=agents ,project_name=project_name)
from prefect_lib.flow.monthly_delete_flow import flow
flow.register(labels=agents ,project_name=project_name)
from prefect_lib.flow.mongo_export_selector_flow import flow
flow.register(labels=agents ,project_name=project_name)
from prefect_lib.flow.mongo_import_selector_flow import flow
flow.register(labels=agents ,project_name=project_name)

# ENTRY系
from prefect_lib.flow.scraper_info_uploader_flow import flow
flow.register(labels=agents ,project_name=project_name)
from prefect_lib.flow.stop_controller_update_flow import flow
flow.register(labels=agents ,project_name=project_name)
from prefect_lib.flow.regular_observation_controller_update_flow import flow
flow.register(labels=agents ,project_name=project_name)

# stats系
from prefect_lib.flow.scraper_pattern_report_flow import flow
flow.register(labels=agents ,project_name=project_name)
from prefect_lib.flow.stats_analysis_report_flow import flow
flow.register(labels=agents ,project_name=project_name)
from prefect_lib.flow.stats_info_collect_flow import flow
flow.register(labels=agents ,project_name=project_name)
