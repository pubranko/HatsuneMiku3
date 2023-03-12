from dataclasses import dataclass,field
from urllib.parse import urlparse

'''このソースは現在未使用'''

@dataclass
class AsynchronousReportTotalizationData:
    data: dict = field(default_factory=dict)
    # data: dict[str, dict[str, dict[str, int]]] = {
    # }
        # データイメージ
        # 'レコードタイプ①':{
        #     'by_record_type':{'count':0},
        #     'by_domain':{'ドメイン①':0, 'ドメイン②':0},}
        # 'レコードタイプ②':{
        #     'by_record_type':{'count':0},
        #     'by_domain':{'ドメイン①':0, 'ドメイン②':0},}

    def record_type_get(self, record_type: str) -> dict:
        '''
        引数に指定されたレコードタイプの辞書を返す。
        存在しない場合は空の辞書を返す。
        '''
        if record_type in self.data:
            return self.data[record_type]
        else:
            return {}

    def record_type_set(self, record_type: str) -> None:
        '''
        引数に指定されたレコードタイプを新たに保存する（初期設定）。
        ※主にrecord_type_getメソッドで空の辞書が帰ってきた場合に使用する。
        '''
        self.data[record_type] = {
            'by_record_type': {'count': 0},
            'by_domain': {}, }

    def record_type_counter(self, record_type: str) -> None:
        '''
        レコード別のカウント
        '''
        self.data[record_type]['by_record_type']['count'] += 1

    def by_domain_counter(self, record_type: str,url_list: list) -> None:
        '''
        ドメイン別カウント
        '''
        for url in url_list:
            url_parse = urlparse(url)
            # 新しいドメインの場合は初期化する。
            if url_parse.netloc not in self.data[record_type]['by_domain']:
                self.data[record_type]['by_domain'][url_parse.netloc] = 0
            self.data[record_type]['by_domain'][url_parse.netloc] += 1
