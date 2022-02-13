import os
import sys
from datetime import datetime
from logging import Logger
from pydantic import BaseModel, ValidationError, validator, Field
from prefect.engine import state
from prefect.engine.runner import ENDRUN

path = os.getcwd()
sys.path.append(path)
from prefect_lib.task.extentions_task import ExtensionsTask
from prefect_lib.run import scrapying_run, scraped_news_clip_master_save_run, solr_news_clip_save_run
from prefect_lib.data_models.log_analysis_report_model import LogAnalysisReportModel


class LogAnalysisReportTask(ExtensionsTask):
    '''
    '''

    def run(self, report_term: str, base_date: datetime, **kwargs):
        '''ここがprefectで起動するメイン処理'''

        #kwargs['start_time'] = self.start_time
        #kwargs['mongo'] = self.mongo
        logger: Logger = self.logger
        logger.info('=== ScrapyingTask run kwargs : ' + str(kwargs))

        try:
            validator = LogAnalysisReportModel(
                start_time=self.start_time,
                report_term=report_term,
                base_date=base_date,
            )
        except ValidationError as e:
            # print(e.json())  # エラー結果をjson形式で見れる。
            print('エラー内容 : ', e.errors())  # エラー結果をlist形式で見れる。
            # print(str(e))  # エラー結果をlist形式で見れる。
            raise ENDRUN(state=state.Failed())

        print('=== validator')
        print(validator.report_term,validator.base_date)
        a = validator.base_date_get()
        print(a)

        # scrapying_run.exec(kwargs)

        # if kwargs['following_processing_execution'] == 'Yes':
        #     # 必要な引数設定
        #     kwargs['scrapying_start_time_from'] = self.start_time
        #     kwargs['scrapying_start_time_to'] = self.start_time
        #     kwargs['scraped_save_start_time_from'] = self.start_time
        #     kwargs['scraped_save_start_time_to'] = self.start_time

        #     scraped_news_clip_master_save_run.check_and_save(kwargs)
        #     solr_news_clip_save_run.check_and_save(kwargs)

        # 終了処理
        self.closed()
        # return ''
