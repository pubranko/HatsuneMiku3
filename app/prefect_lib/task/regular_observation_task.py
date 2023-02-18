# pylint: disable=E1101
import os
import sys
import threading
path = os.getcwd()
sys.path.append(path)
from prefect_lib.task.extentions_task import ExtensionsTask
from prefect_lib.run import scrapy_crawling_run, scrapying_run, scraped_news_clip_master_save_run, solr_news_clip_save_run
from models.controller_model import ControllerModel
from common_lib.directory_search_spiders import DirectorySearchSpiders
from prefect_lib.data_models.scrapy_crawling_kwargs_input import ScrapyCrawlingKwargsInput
from BrownieAtelierStorage.models.controller_blob_model import ControllerBlobModel


class RegularObservationTask(ExtensionsTask):
    '''
    定期観測用タスク
    '''

    def run(self):
        '''ここがprefectで起動するメイン処理'''
        self.run_init()

        directory_search_spiders = DirectorySearchSpiders()
        controller: ControllerModel = ControllerModel(self.mongo)
        spider_name_set: set = controller.regular_observation_spider_name_set_get()
        stop_domain: list = controller.crawling_stop_domain_list_get()

        # 定期観測の対象スパイダー情報、スパイダー名称保存リスト
        crawling_target_spiders: list = []
        crawling_target_spiders_name: list = []

        # 対象スパイダーの絞り込み
        for spider_name in directory_search_spiders.spiders_name_list_get():
            spider_info = directory_search_spiders.spiders_info[spider_name]
            crawl_point_record: dict = controller.crawl_point_get(
                spider_info['domain_name'], spider_name,)

            if spider_info['domain'] in stop_domain:
                self.logger.info(
                    f'=== Stop domainの指定によりクロール中止 : ドメイン({spider_info["domain"]}) : spider_name({spider_name})')
            elif not spider_name in spider_name_set:
                self.logger.info(
                    f'=== 定期観測に登録がないスパイダーは対象外 : ドメイン({spider_info["domain"]}) : spider_name({spider_name})')
            elif len(crawl_point_record) == 0:
                self.logger.info(
                    f'=== クロールポイントがない（初回未実行）スパイダーは対象外 : ドメイン({spider_info["domain"]}) : spider_name({spider_name})')
            else:
                crawling_target_spiders.append(spider_info)
                crawling_target_spiders_name.append(spider_name)

        # spider_kwargsで指定された引数より、scrapyを実行するための引数へ補正を行う。
        scrapy_crawling_kwargs_input = ScrapyCrawlingKwargsInput({
            'continued': 'Yes', })

        self.logger.info(f'=== 定期観測対象スパイダー : {str(crawling_target_spiders_name)}')
        self.logger.info(f'=== 定期観測 run kwargs : {scrapy_crawling_kwargs_input.spider_kwargs_correction()}')

        thread = threading.Thread(
            target=scrapy_crawling_run.custom_runner_run(
                logger=self.logger,
                start_time=self.start_time,
                scrapy_crawling_kwargs=scrapy_crawling_kwargs_input.spider_kwargs_correction(),
                spiders_info=crawling_target_spiders))

        # マルチプロセスで動いているScrapyの終了を待ってから後続の処理を実行する。
        thread.start()
        thread.join()

        kwargs: dict = {}
        kwargs['start_time'] = self.start_time
        kwargs['mongo'] = self.mongo
        kwargs['domain'] = None
        kwargs['urls'] = []
        kwargs['crawling_start_time_from'] = self.start_time
        kwargs['crawling_start_time_to'] = self.start_time
        kwargs['scrapying_start_time_from'] = self.start_time
        kwargs['scrapying_start_time_to'] = self.start_time
        kwargs['scraped_save_start_time_from'] = self.start_time
        kwargs['scraped_save_start_time_to'] = self.start_time

        scrapying_run.exec(kwargs)
        scraped_news_clip_master_save_run.check_and_save(kwargs)

        ### 本格開発までsolrへの連動を一時停止 ###
        # solr_news_clip_save_run.check_and_save(kwargs)

        # 終了処理
        self.closed()

        # 定期観測終了後コンテナーを停止させる。
        #   azure functions BLOBトリガーを動かすためのBLOBファイルを削除＆作成を実行する。
        controller_blob_model = ControllerBlobModel()
        controller_blob_model.delete_blob()
        controller_blob_model.upload_blob()
        # return ''
