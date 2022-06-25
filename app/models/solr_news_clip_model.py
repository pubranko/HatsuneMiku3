from datetime import datetime
from dateutil import parser
import pysolr
import os
from typing import Union
from collections.abc import ItemsView
import logging
from logging import Logger

# solrの特殊文字
#   + - && || ! ( ) { } [ ] ^ " ~ * ? : \


class SolrNewsClip(object):
    """
    solr用のモデル。solrからのデータの取得を行う。
    """
    solr: pysolr.Solr
    logger: Logger

    def __init__(self,logger) -> None:
        """
        solrへ接続したインスタンスを返す。
        """
        # 環境変数からsolrの情報を取得するように変更
        self.solr = pysolr.Solr(
            os.environ['SOLR_URL'] + os.environ['SOLR_CORE'],
            timeout=30,
            verify='',
            # verify=SolrEnv.VERIFY,     #solrの公開鍵を指定する場合、ここにファイルパスを入れる。
            auth=(os.environ['SOLR_WRITE_USER'],
                  os.environ['SOLR_WRITE_PASS']),
            always_commit=True,
        )

        self.logger = logger

    def search_query(
        self,
        search_query: list,
        skip: int = 0,
        limit: int = 100,
        sort: dict = {'response_time': 'desc', },
        field: list = [],
        facet: str = 'on',
        facet_field: list = [],
    ) -> Union[pysolr.Results, None]:
        """
        引数で渡されたrequest内のsolrサーチ用の情報より、検索を実行しレスポンスを返す。
        """
        search_query_str = ''.join(search_query)
        #self.logger.info('=== solrへqueryを送信:' + str(search_query_str))

        # sort用のdictをsolr用に変換
        i: ItemsView = sort.items()
        sort_list: list = []
        for t in i:
            sort_list.append(t[0] + ' ' + t[1] + ',')

        try:
            results = self.solr.search([search_query_str], **{
                'rows': limit,
                'start': skip,
                # ソートのやり方。 desc降順 asc昇順。 %20は空白に置き換えること。'response_time desc,',
                'sort': ''.join(sort_list),
                # 取得したフィールドを限定したい場合
                'fl': ','.join(field),
                # ファセット、取得したいフィールド
                'facet': facet,
                'facet_field': ','.join(facet_field),
            })
            return results

        except Exception as e:
            self.logger.critical('=== solrとの通信でエラーが発生 ===' + str(e))
            return None

        """ pysolr.Resultsの内部構造は以下の通り。
        ('__class__', <class 'pysolr.Results'>)
        {'raw_response':
            {'responseHeader': {'status': 0, 'QTime': 0, 'params': {'q': '*:*', 'start': '0', 'rows': '1', 'wt': 'json'}},
            'response': {'numFound': 7427, 'start': 0,
                        'docs': [{'mongo_id': ['〜'], 'url': '〜', 'title': '〜', 'article': '〜', 'issuer': ['〜'], 'update_count': 0, 'id': '〜', '_version_': 1638412764864053248, 'response_time': '2019-01-20T19:24:59.014Z', 'publish_date': '2018-04-01T00:00:00Z'}]}
            },
        'docs': [{'mongo_id': ['〜'], 'url': '〜', 'title': '〜', 'article': '〜', 'issuer': ['〜'], 'update_count': 0, 'id': '〜', '_version_': 1638412764864053248,'response_time': '2019-01-20T19:24:59.014Z', 'publish_date': '2018-04-01T00:00:00Z'}],
        'hits': 7427, 'debug': {}, 'highlighting': {}, 'facets': {}, 'spellcheck': {},
        'stats': {}, 'qtime': 0, 'grouped': {}, 'nextCursorMark': None, '_next_page_query': None
        }
        """

    def escape_convert(self, word: str) -> str:
        # solrの特殊文字
        #   + - ! ( ) { } [ ] ^ " ~ * ? : \ && ||
        table = str.maketrans({
            '+': r'\+',
            '-': r'\-',
            '!': r'\!',
            '(': r'\(',
            ')': r'\)',
            '[': r'\[',
            ']': r'\]',
            '{': r'\{',
            '}': r'\}',
            '^': r'\^',
            '"': r'\"',
            '~': r'\~',
            '*': r'\*',
            '?': r'\?',
            ':': r'\:',
            # '&&': r'\&&',
            # '||': r'\||',
            # '\\': r'\\',
        })
        return word.translate(table)

    def results_check(self, results: pysolr.Results):
        """
        リザルトチェック。レコードの内容をチェックし、スクレイピングに失敗してレコードがあれば不備の文言を設定。
        ※後日バリデーターの導入を検討予定。
        """
        recodes_count = results.raw_response['response']['numFound']
        recodes = []
        for recode in results.docs:
            try:
                # 項目の存在チェック
                recode['url']
                recode['title']
                recode['article']
                recode['publish_date']
                recode['issuer']
                recode['update_count']
            except:
                recodes.append({
                    'title': '登録データに欠損があるため、こちらのurlの情報は正しく表示できない状態です。',
                    'article': '',
                    'url': recode['url'],
                    'publish_date': '',
                    'issuer': '',
                    'update_count': '',
                })
            else:
                recodes.append({
                    'title': recode['title'],
                    'article': recode['article'],
                    'url': recode['url'],
                    'publish_date': datetime.strftime(parser.parse(recode['publish_date']), '%Y-%m-%d %H:%M'),
                    'issuer': recode['issuer'][0],
                    'update_count': recode['update_count'],
                })

        return {'recodes_count': recodes_count, 'recodes': recodes}

    def article_cut(self, results_check: dict):
        """
        本文を最長５０文字でカットする。（全文転載とさせないため）
        入力の条件：results_checkメソッドの戻り値の形式であること。
        """
        recodes = []
        for recode in results_check["recodes"]:
            # 本文は一部のみ表示する。（全文表示すると無断転載になる）
            article_part_len = len(recode['article']) // 3  # 3で割って整数部のみ取得
            if article_part_len > 50:  # 本文の3割以下＆最大50文字
                recode['article'] = recode['article'][0:50]
            else:
                recode['article'] = recode['article'][0:article_part_len]
            recodes.append(recode)

        results_check["recodes"] = recodes
        return results_check

    def add(self, record: dict):

        self.solr.add([{
            "url": record['url'],
            "title": record['title'],
            "article": record['article'],
            "response_time": record['response_time'].isoformat(),
            "publish_date": record['publish_date'].isoformat(),
            "issuer": record['issuer'],
            "update_count": 0,
        }, ])

    def delete_id(self, delete_id_list: list):
        self.solr.delete(id=delete_id_list)

    def delete_query(self, delete_query):
        self.solr.delete(q=delete_query)

    def commit(self):
        self.solr.commit()
