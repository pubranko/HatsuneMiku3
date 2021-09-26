# pylint: disable=E1101
import os
import sys
from typing import Any
from logging import Logger
from prefect.engine import state
from prefect.engine.runner import ENDRUN
path = os.getcwd()
sys.path.append(path)
from common_lib.directory_search import directory_search
from prefect_lib.task.extentions_task import ExtensionsTask
from prefect_lib.run import scrapy_crawling_run
from prefect_lib.task.extentions_task import ExtensionsTask


class ScrapyCrawlingTask(ExtensionsTask):
    '''
    クローリング用タスク
    '''

    # def run(self, module, method):
    #     '''ここがprefectで起動するメイン処理'''
    #     mod: Any = import_module(module)
    #     getattr(mod, method)(self.start_time)
    #     # 終了処理
    #     self.closed()
    #     # return ''

    #def run(self, spider_names:list,spider_kwargs:dict):
    def run(self, **kwargs):
        '''ここがprefectで起動するメイン処理'''
        #print(type(spider_names))
        #print(type(spider_kwargs))

        logger: Logger = self.logger
        #kwargs['start_time'] = self.start_time
        kwargs['logger'] = self.logger
        logger.info('=== Scrapy crawling run kwargs : ' + str(kwargs))

        #kwargs['spider_names']
        #kwargs['spider_kwargs']

        error_spider_names: list = []
        spider_run_list:list = []

        spiders_info: list = directory_search()
        spiders_info_name_list = [x['spider_name'] for x in spiders_info]

        # 引数で渡されたスパイダー名リストを順に処理
        for spider_name in kwargs['spider_names']:
            # spidersディレクトリより取得した一覧に存在するかチェック
            if spider_name in spiders_info_name_list:
                # クラスインスタンスリストへ追加
                i = spiders_info_name_list.index(spider_name)
                spider_run = {'spider_name' : spider_name,
                            'class_instans' : spiders_info[i]['class_instans'],
                            'start_time' : self.start_time,
                }
                if  'lastmod_period_minutes' in kwargs['spider_kwargs']:
                    spider_run['lastmod_period_minutes'] = kwargs['spider_kwargs']['lastmod_period_minutes']
                else:
                    spider_run['lastmod_period_minutes'] = None
                if  'pages' in kwargs['spider_kwargs']:
                    spider_run['pages'] = kwargs['spider_kwargs']['pages']
                else:
                    spider_run['pages'] = None
                if  'continued' in kwargs['spider_kwargs']:
                    spider_run['continued'] = kwargs['spider_kwargs']['continued']
                else:
                    spider_run['continued'] = None
                if  'direct_crawl_urls' in kwargs['spider_kwargs']:
                    spider_run['direct_crawl_urls'] = kwargs['spider_kwargs']['direct_crawl_urls']
                else:
                    spider_run['direct_crawl_urls'] = None
                if  'debug' in kwargs['spider_kwargs']:
                    spider_run['debug'] = kwargs['spider_kwargs']['debug']
                else:
                    spider_run['debug'] = None
                if  'crawl_point_non_update' in kwargs['spider_kwargs']:
                    spider_run['crawl_point_non_update'] = kwargs['spider_kwargs']['crawl_point_non_update']
                else:
                    spider_run['crawl_point_non_update'] = None

                spider_run_list.append(spider_run)

            else:
                error_spider_names.append(spider_name)

        if len(error_spider_names):
            logger.error(
                '=== scrapy crwal run : 指定されたspider_nameは存在しませんでした : ' + str(error_spider_names))
            raise ENDRUN(state=state.Failed())
        else:
            kwargs['spider_run_list'] = spider_run_list
            scrapy_crawling_run.custom_crawl_run(kwargs)

        self.closed()
