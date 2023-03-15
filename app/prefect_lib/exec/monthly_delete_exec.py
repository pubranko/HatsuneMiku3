from prefect_lib.flow.monthly_delete_flow import flow
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
        ControllerModel.COLLECTION_NAME,
    ],
    delete_period_from='2022-02',  # 月次削除を行うデータの基準年月
    delete_period_to='2022-05',  # 月次削除を行うデータの基準年月
))
