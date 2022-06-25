import os
import fasteners


class CrawlingDomainDuplicatePrevention():
    '''
    同一ドメインへの多重クローリングを防止する。(Linux限定モジュール。Windowsでは使用できない。)
    '''

    def execution(self, domain_name: str) -> bool:
        '''
        以下のファイルを排他制御で確保する。
        news_crawl/exclusive_work/ドメイン名.txt
        '''
        path = os.path.join(
            'news_crawl', 'exclusive_work', domain_name + '.txt')
        self.lock = fasteners.InterProcessLock(path)
        result: bool = self.lock.acquire(blocking=False)

        return result
