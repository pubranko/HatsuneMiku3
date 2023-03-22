from prefect_lib.flow.mongo_import_selector_flow import flow
from BrownieAtelierMongo.collection_models.crawler_response_model import CrawlerResponseModel
from BrownieAtelierMongo.collection_models.scraped_from_response_model import ScrapedFromResponseModel
from BrownieAtelierMongo.collection_models.news_clip_master_model import NewsClipMasterModel
from BrownieAtelierMongo.collection_models.crawler_logs_model import CrawlerLogsModel
from BrownieAtelierMongo.collection_models.asynchronous_report_model import AsynchronousReportModel
from BrownieAtelierMongo.collection_models.controller_model import ControllerModel


flow.run(parameters=dict(
    collections_name=[
        CrawlerResponseModel.COLLECTION_NAME,
        ScrapedFromResponseModel.COLLECTION_NAME,
        NewsClipMasterModel.COLLECTION_NAME,
        CrawlerLogsModel.COLLECTION_NAME,
        AsynchronousReportModel.COLLECTION_NAME,
        # ControllerModel.COLLECTION_NAME,
    ],
    prefix='test1',
    backup_dir_from='2023-03',    # prefect_lib.settings.BACKUP_BASE_DIR内のディレクトリを指定
    backup_dir_to='2023-03',    # prefect_lib.settings.BACKUP_BASE_DIR内のディレクトリを指定
))
