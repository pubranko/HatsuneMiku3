from __future__ import annotations
from datetime import datetime
from typing import Any, Tuple
from typing import Generator
import pandas as pd
from copy import deepcopy
import itertools


class ScraperByDomainData:
    '''
    '''

    scraper_by_domain_record: dict = {}

    def __init__(self):
        ''''''
        pass

    def record_recovery(self, scraper_by_domain_record: dict):
        '''引数としてドメインごとのレコードを受け取る。'''
        self.scraper_by_domain_record = scraper_by_domain_record

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
