import os
import io
import sys
import json
from pprint import pprint
import tkinter
from tkinter import Variable, ttk
from tkinter.constants import ANCHOR, CENTER, COMMAND
from tkinter import scrolledtext
import tkcalendar
from functools import partial
from typing import Any, Generator
from pymongo.cursor import Cursor
from pymongo import DESCENDING
from dateutil import parser
path = os.getcwd()
sys.path.append(path)
from models.mongo_model import MongoModel
from models.crawler_logs_model import CrawlerLogsModel
from common_lib.timezone_recovery import timezone_recovery
from common_lib.directory_search_task import directory_search_task

'''
ログフィルターの入力欄が必要
    ドメイン
    時間from〜to(日本時間)
    ログの種類(record_type) spider_reports, ScrapyCrawlingTask, ScrapyingTask, ほか多数

    一覧表示エリア ＆ 各明細それぞれの表示ボタン
    ページ遷移用ボタン
    フィルターにマッチしたログを一括でlogs/〜.logファイルへ保存するボタン

    明細の表示(サブウィンドウ）
    logs/~.logファイルに保存するボタン
'''


class LogViewer(tkinter.Frame):
    def __init__(self, root: tkinter.Tk):
        super().__init__(root, width=1000, height=600, borderwidth=1, relief='groove')

        root.title('Log Viewer')
        self.root = root
        self.pack()
        self.pack_propagate(False)
        self.create_widgets()

        mongo = MongoModel()
        self.crawler_log = CrawlerLogsModel(mongo)

    def create_widgets(self):
        '''
        初期画面表示
        '''
        # menu
        menu = tkinter.Menu(self.root)
        menu1 = tkinter.Menu(menu, tearoff=False)
        menu1.add_command(label='test1', command='あとで')
        menu1.add_command(label='test2', command='あとで')
        menu.add_cascade(label='メニュー', menu=menu1)
        self.root.config(menu=menu)

        # start_time_from
        date_from_label = tkinter.LabelFrame(
            self, text='start_time(from)')
        self.date_from = tkinter.Entry(date_from_label)
        self.time_from = tkinter.Entry(date_from_label, width=8,)
        date_from_label.grid(row=0, column=0, )
        self.date_from.pack(side=tkinter.LEFT)
        self.time_from.pack(side=tkinter.LEFT)

        # start_time_to
        date_to_label = tkinter.LabelFrame(
            self, text='start_time(to)')
        self.date_to = tkinter.Entry(date_to_label)
        self.time_to = tkinter.Entry(date_to_label, width=8)
        date_to_label.grid(row=1, column=0)
        self.date_to.pack(side=tkinter.LEFT)
        self.time_to.pack(side=tkinter.LEFT)

        # record_type
        # prefectのTaskディレクトリより取得。
        record_type_list = [str(x['class_name'])
                            for x in directory_search_task()]
        # その他個別にログに保管したtypeを追加。
        '''ちょっとこれはあとで見直ししよう、、、'''
        record_type_list.extend(['spider_reports','news_crawl_asy','news_clip_master_async','solr_news_clip_async',])

        record_type_list.sort()
        self.record_type_label = tkinter.LabelFrame(
            self, text='record_type')

        self.yscroll = tkinter.Scrollbar(self.record_type_label, orient=tkinter.VERTICAL)
        self.yscroll.pack(side=tkinter.RIGHT, fill=tkinter.Y)
        self.record_type = tkinter.Listbox(
            self.record_type_label,
            selectmode= tkinter.EXTENDED,   #リストボックスを複数選択可能とする。
            height=5,
            width=max([len(x) for x in record_type_list]),
            yscrollcommand=self.yscroll.set,
            )
        for rec in record_type_list:
            self.record_type.insert(tkinter.END, rec)

        self.record_type_label.grid(row=0, rowspan=3,column=1)
        self.record_type.pack()

        # ログ検索実行ボタン
        self.log_list_get_button = tkinter.Button(
            self, text="ログ一覧取得", command=self.log_list_view)
        self.log_list_get_button.grid(
            row=2, column=0, sticky=tkinter.EW)

    def log_list_view(self):
        '''抽出条件を満たすログのリストを表示'''
        from pydantic import BaseModel, ValidationError, validator
        from GUI.tkinter.log_viewer_validator import LogViewerValidator

        try:
            condition_items = LogViewerValidator(
                date_from=self.date_from.get(),
                time_from=self.time_from.get(),
                date_to=self.date_to.get(),
                time_to=self.time_to.get(),
                record_type=[self.record_type.get(i,i)[0] for i in self.record_type.curselection()],
            )
            print(condition_items.dict())
        except ValidationError as e:
            print(e.json())  # エラー結果をjson形式で見れる。
            print(e.errors())  # エラー結果をlist形式で見れる。
            print(str(e))  # エラー結果をlist形式で見れる。
        else:
            conditions: list = []
            # print('条件',condition_items.datetime_from())
            # print('あれ、、、',type(condition_items.))
            # print('あれ、、、',condition_items.date_from)
            if condition_items.date_from:
                conditions.append(
                    {'start_time': {'$gte': condition_items.datetime_from()}})
            if condition_items.date_to:
                conditions.append(
                    {'start_time': {'$lte': condition_items.datetime_to()}})
            if len(condition_items.record_type):
                conditions.append(
                    {'record_type': {'$in': [condition_items.record_type]}})

            if conditions:
                self.filter: Any = {'$and': conditions}
            else:
                self.filter = None

            print('self.filter : ', self.filter)

            # 対象件数を確認
            record_count = self.crawler_log.find(filter=self.filter,).count()
            print('=== 対象件数 : ',record_count)

            # 件数制限で順に取得
            # とりあえず1ページ分だけ。
            if record_count:
                records: Cursor = self.log_list_get(1, record_count)
                # limit: int = 10
                # skip_list = list(range(0, record_count, limit))
                # for skip in skip_list:
                #     records: Cursor = self.crawler_log.find(
                #         filter=self.filter,
                #         sort=[('start_time', DESCENDING)],
                #     ).skip(skip).limit(limit)

                # ログリスト表示エリア
                logs_frame = tkinter.Frame(
                    self, relief="ridge", bd=3, padx=5, pady=5,)
                logs_frame.grid(row=3, column=0, columnspan=4, sticky=tkinter.W)

                # 見出し
                logs_table: list = []
                logs_table.append([
                    tkinter.Label(logs_frame, text='No.', relief='groove'),
                    tkinter.Label(logs_frame, text='mongo_id',
                                relief='groove', width=0),
                    tkinter.Label(logs_frame, text='record_type', relief='groove'),
                    tkinter.Label(logs_frame, text='domain', relief='groove'),
                    tkinter.Label(logs_frame, text='', relief='groove'), ])  # 表示ボタン

                for idx, record in enumerate(records):
                    # レコードの追加
                    logs_table.append([
                        tkinter.Label(
                            logs_frame, text=str(idx + 1), anchor=tkinter.E, relief='ridge'),
                        tkinter.Label(
                            logs_frame, text=record['_id'], anchor=tkinter.W, relief='ridge', width=0),
                        tkinter.Label(
                            logs_frame, text=record['record_type'], anchor=tkinter.W, relief='ridge'),
                        tkinter.Label(
                            logs_frame, text=record['domain'] if 'domain' in record else '', anchor=tkinter.W, relief='ridge'),
                        tkinter.Button(
                            logs_frame, text='詳細表示', pady=0,
                            command=partial(self.log_view, record['_id'])
                        )
                    ])

                for row_idx, col_items in enumerate(logs_table):
                    for col_idx, col_item in enumerate(col_items):
                        col_item.grid(row=row_idx, column=col_idx,
                                    sticky=tkinter.EW)

    def log_view(self, mongo_id):
        '''ログの明細を表示'''
        record = self.log_get(mongo_id)
        # サブ画面
        log_window = tkinter.Toplevel()
        log_window.title("log view")
        # log_window.geometry("900x500")

        key_object: list = []
        value_object: list = []
        for idx, (key, value) in enumerate(record.items()):
            key_object.append(tkinter.Entry(log_window,))
            k: tkinter.Entry = key_object[idx]
            k.insert(tkinter.END, str(key))
            k.grid(row=idx, column=0, sticky=tkinter.NW, ipadx=30)

            # valueを種類に応じて表示
            if key == 'logs':
                # 改行含む場合
                value_object.append(scrolledtext.ScrolledText(
                    log_window, wrap=tkinter.WORD, width=200))
                value_object[idx].insert(tkinter.END, str(value))
                value_object[idx].grid(row=idx, column=1)
            elif key in ['stats', 'crawl_urls_list']:
                # 整形して表示したい場合
                file_like = io.StringIO('')
                pprint(value, stream=file_like)

                value_object.append(scrolledtext.ScrolledText(
                    log_window, wrap=tkinter.WORD, width=200, height=10))
                value_object[idx].insert(
                    tkinter.END, str(file_like.getvalue()))
                value_object[idx].grid(row=idx, column=1)
            else:
                value_object.append(tkinter.Entry(log_window))
                value_object[idx].insert(tkinter.END, str(value))
                value_object[idx].grid(
                    row=idx, column=1, sticky=tkinter.EW, ipadx=500)

    def log_list_get(self, page: int, record_count: int) -> Cursor:
        # 件数制限で順に取得
        limit: int = 10
        skip_list = list(range(0, record_count, limit))

        records: Cursor = self.crawler_log.find(
            filter=self.filter,
            sort=[('start_time', DESCENDING)],
        ).skip(skip_list[page - 1]).limit(limit)

        return records

    def log_get(self, mondo_id: str) -> Any:
        '''ログを１件取得'''
        record = self.crawler_log.find_one(filter={'_id': mondo_id},)
        return record


##################################################
##################################################
##################################################

if __name__ == "__main__":
    root: tkinter.Tk = tkinter.Tk()
    app = LogViewer(root=root)

    root.mainloop()
