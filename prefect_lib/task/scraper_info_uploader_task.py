import os
import sys
import pickle
import glob
import re
import json
from typing import Any, Union
from logging import Logger
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
from pymongo import ASCENDING
from pymongo.cursor import Cursor
from prefect.engine import state
from prefect.engine.runner import ENDRUN
path = os.getcwd()
sys.path.append(path)
from prefect_lib.settings import TIMEZONE, BACKUP_BASE_DIR, SCRAPER_INFO_BY_DOMAIN_DIR
from prefect_lib.task.extentions_task import ExtensionsTask
from prefect_lib.data_models.scraper_info_by_domain_data import ScraperInfoByDomainData
from models.scraper_info_by_domain_model import ScraperInfoByDomainModel


class ScraperInfoUploaderTask(ExtensionsTask):
    '''
    '''
    def run(self, **kwargs):
        ''''''
        logger: Logger = self.logger
        logger.info(f'=== ScraperInfoUploaderTask run kwargs : {str(kwargs)}')

        scraper_info_by_domain_model = ScraperInfoByDomainModel(self.mongo)

        #scraper_info_by_domain_files: list = kwargs['scraper_info_by_domain_files']
        scraper_info_by_domain_files:list = []
        files:list = kwargs['scraper_info_by_domain_files']
        if len(kwargs['scraper_info_by_domain_files']) == 0:
            path = os.path.join(SCRAPER_INFO_BY_DOMAIN_DIR, '*.json')
            scraper_info_by_domain_files = glob.glob(path)
            logger.info(
                f'=== ScraperInfoUploaderTask run ファイル指定なし → 全ファイル対象 : {scraper_info_by_domain_files}')
        else:
            for file in files:
                scraper_info_by_domain_files.append(os.path.join(SCRAPER_INFO_BY_DOMAIN_DIR, file))

        if len(scraper_info_by_domain_files) == 0:
            raise ENDRUN(state=state.Failed())

        for file_name in scraper_info_by_domain_files:
            with open(file_name, 'r') as f:
                file = f.read()

            scraper_info:dict = json.loads(file)
            scraper_info_by_domain_data = ScraperInfoByDomainData(scraper=scraper_info)
            scraper_info_by_domain_model.update(
                filter={'domain': scraper_info['domain']},
                record=scraper_info)

            # 処理の終わったファイルオブジェクトを削除
            del file, scraper_info

        # 終了処理
        self.closed()
        # return ''
