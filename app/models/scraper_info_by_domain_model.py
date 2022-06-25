import os
from models.mongo_model import MongoModel
from models.mongo_common_model import MongoCommonModel
from typing import Any, Tuple
from typing import Generator


class ScraperInfoByDomainModel(MongoCommonModel):
    '''
    scraper_by_domainコレクション用モデル
    '''
    mongo: MongoModel
    collection_name: str = os.environ['MONGO_SCRAPER_BY_DOMAIN']

    scraper_by_domain_record: dict = {}

    def record_read(self, filter) -> None:
        ''' '''
        record = self.find_one(filter=filter)
        if type(record) is dict:
            self.scraper_by_domain_record = record
        else:
            self.scraper_by_domain_record = {}

    def domain_get(self) -> str:
        '''ドメインを返す'''
        return self.scraper_by_domain_record['domain']

    def scrape_item_get(self) -> Generator[Tuple[str, list[dict[str, str]]], None, None]:
        '''
        scrape_itemsを返すジェネレーター
        '''
        scrape_items: dict[str, list[dict[str, str]]
                           ] = self.scraper_by_domain_record['scrape_items']
        for scraper, pattern_list in scrape_items.items():
            # patternリストは、patternで降順にソート
            pattern_list = sorted(pattern_list,
                        key=lambda d: d['pattern'], reverse=True)
            yield scraper, pattern_list
