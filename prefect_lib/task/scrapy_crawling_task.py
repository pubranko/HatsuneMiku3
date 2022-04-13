# pylint: disable=E1101
import os
import sys
from prefect.engine import state
from prefect.engine.runner import ENDRUN
import threading
path = os.getcwd()
sys.path.append(path)
from common_lib.directory_search_spiders import directory_search_spiders
from prefect_lib.task.extentions_task import ExtensionsTask
from prefect_lib.run import scrapy_crawling_run, scrapying_run, scraped_news_clip_master_save_run, solr_news_clip_save_run
from prefect_lib.task.extentions_task import ExtensionsTask


class ScrapyCrawlingTask(ExtensionsTask):
    '''
    クローリング用タスク
    '''

    def run(self, **kwargs):
        '''ここがprefectで起動するメイン処理'''
        kwargs['logger'] = self.logger
        self.logger.info('=== Scrapy crawling run kwargs : ' + str(kwargs))

        error_spider_names: list = []
        spider_run_list: list = []
        spider_run_threads: list = []

        spiders_info: list = directory_search_spiders()
        spiders_info_name_list = [x['spider_name'] for x in spiders_info]

        # 引数で渡されたスパイダー名リストを順に処理(重複があった場合はsetで削除)
        spider_names_set = set(kwargs['spider_names'])
        for spider_name in spider_names_set:
            # spidersディレクトリより取得した一覧に存在するかチェック
            if spider_name in spiders_info_name_list:
                # クラスインスタンスリストへ追加
                idx = spiders_info_name_list.index(spider_name)
                spider_run = {'spider_name': spider_name,
                              'class_instans': spiders_info[idx]['class_instans'],
                              'start_time': self.start_time,
                              }
                # spider引数を設定
                if 'lastmod_period_minutes' in kwargs['spider_kwargs']:
                    spider_run['lastmod_period_minutes'] = kwargs['spider_kwargs']['lastmod_period_minutes']
                else:
                    spider_run['lastmod_period_minutes'] = None
                if 'pages' in kwargs['spider_kwargs']:
                    spider_run['pages'] = kwargs['spider_kwargs']['pages']
                else:
                    spider_run['pages'] = None
                if 'continued' in kwargs['spider_kwargs']:
                    spider_run['continued'] = kwargs['spider_kwargs']['continued']
                else:
                    spider_run['continued'] = None
                if 'direct_crawl_urls' in kwargs['spider_kwargs']:
                    spider_run['direct_crawl_urls'] = kwargs['spider_kwargs']['direct_crawl_urls']
                else:
                    spider_run['direct_crawl_urls'] = None
                if 'debug' in kwargs['spider_kwargs']:
                    spider_run['debug'] = kwargs['spider_kwargs']['debug']
                else:
                    spider_run['debug'] = None
                if 'crawl_point_non_update' in kwargs['spider_kwargs']:
                    spider_run['crawl_point_non_update'] = kwargs['spider_kwargs']['crawl_point_non_update']
                else:
                    spider_run['crawl_point_non_update'] = None

                spider_run_list.append(spider_run)

            else:
                error_spider_names.append(spider_name)

        if len(error_spider_names):
            self.logger.error(
                '=== scrapy crwal run : 指定されたspider_nameは存在しませんでした : ' + str(error_spider_names))
            raise ENDRUN(state=state.Failed())
        else:
            kwargs['spider_run_list'] = spider_run_list
            thread = threading.Thread(
                target=scrapy_crawling_run.custom_crawl_run(kwargs))
            # マルチプロセスで動いているScrapyの終了を待ってから後続の処理を実行する。
            thread.start()
            thread.join()

        if kwargs['following_processing_execution'] == 'Yes':
            # 必要な引数設定
            kwargs['start_time'] = self.start_time
            kwargs['mongo'] = self.mongo
            kwargs['domain'] = None
            kwargs['crawling_start_time_from'] = self.start_time
            kwargs['crawling_start_time_to'] = self.start_time
            kwargs['urls'] = []
            kwargs['scrapying_start_time_from'] = self.start_time
            kwargs['scrapying_start_time_to'] = self.start_time
            kwargs['scraped_save_start_time_from'] = self.start_time
            kwargs['scraped_save_start_time_to'] = self.start_time

            scrapying_run.exec(kwargs)
            scraped_news_clip_master_save_run.check_and_save(kwargs)

            ### 本格開発までsolrへの連動を一時停止 ###
            # solr_news_clip_save_run.check_and_save(kwargs)

        self.closed()
