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
path = os.getcwd()
sys.path.append(path)
from models.mongo_model import MongoModel
from models.crawler_logs_model import CrawlerLogsModel
from common_lib.timezone_recovery import timezone_recovery


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
            self,
            text='start_time(from)'
        )
        self.date_from = tkcalendar.DateEntry(date_from_label)
        self.time_from = tkinter.Entry(date_from_label, width=8,)
        date_from_label.grid(row=0, column=0, )
        self.date_from.pack(side=tkinter.LEFT)
        self.time_from.pack(side=tkinter.LEFT)

        # start_time_to
        date_to_label = tkinter.LabelFrame(
            self,
            text='start_time(to)'
        )
        self.date_to = tkcalendar.DateEntry(date_to_label)
        self.time_to = tkinter.Entry(date_to_label, width=8)
        date_to_label.grid(row=0, column=1)
        self.date_to.pack(side=tkinter.LEFT)
        self.time_to.pack(side=tkinter.LEFT)

        # log_type
        record_type_list = ['ScrapyCrawlingTask', 'spider_reports']
        self.record_type_label = tkinter.LabelFrame(
            self, text='record_type')
        self.log_type = ttk.Combobox(
            self.record_type_label, values=record_type_list)

        self.record_type_label.grid(row=0, column=2)
        self.log_type.pack()

        # ログ検索実行ボタン
        self.log_list_get_button = tkinter.Button(
            self, text="ログ一覧取得", command=self.log_list_view)
        self.log_list_get_button.grid(
            row=1, rowspan=1,
            column=0, columnspan=1,
            sticky=tkinter.EW)

    def log_list_view(self):

        conditions: list = []
        # if domain:
        #     conditions.append({'domain': domain})
        # if crawling_start_time_from:
        #     conditions.append(
        #         {'crawling_start_time': {'$gte': crawling_start_time_from}})
        # if crawling_start_time_to:
        #     conditions.append(
        #         {'crawling_start_time': {'$lte': crawling_start_time_to}})
        # if urls:
        #     conditions.append({'url': {'$in': urls}})
        # if len(stop_domain) > 0:
        #     conditions.append({'domain': {'$nin': stop_domain}})

        if conditions:
            self.filter: Any = {'$and': conditions}
        else:
            self.filter = None

        # 対象件数を確認
        record_count = self.crawler_log.find(
            filter=self.filter,
        ).count()

        records: Cursor = self.log_list_get(1, record_count)

        # ログリスト表示エリア
        logs_frame = tkinter.Frame(self,
                                   relief="ridge", bd=3,
                                   padx=5, pady=5,
                                   )
        logs_frame.grid(row=2, column=0, columnspan=4, sticky=tkinter.W)

        # 見出し
        logs_table: list = []
        logs_table.append([
            tkinter.Label(logs_frame, text='No.', relief='groove'),
            tkinter.Label(logs_frame, text='mongo_id',
                          relief='groove', width=0),
            tkinter.Label(logs_frame, text='record_type', relief='groove'),
            tkinter.Label(logs_frame, text='domain', relief='groove'),
            tkinter.Label(logs_frame, text='', relief='groove'),  # 表示ボタン
        ])
        #
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
                    logs_frame, text=record['_id'], pady=0,
                    command=partial(self.log_view, record['_id'])
                )
            ])

            # record['record_type']
            # record['domain'] if 'domain' in record else ''

        for row_idx, col_items in enumerate(logs_table):
            for col_idx, col_item in enumerate(col_items):
                col_item.grid(row=row_idx, column=col_idx, sticky=tkinter.EW)
            # item[0].grid(row=idx, column=0, sticky=tkinter.EW)
            # item[1].grid(row=idx, column=1, sticky=tkinter.EW)
            # item[2].grid(row=idx, column=2, sticky=tkinter.EW)
            # item[3].grid(row=idx, column=3, sticky=tkinter.EW)

        # 続きの検索ボタン
        # self.log_list_get_button = tkinter.Button(
        #     logs_window, text="ログ一覧取得", command=lambda: self.temp_log_list_get())
        # self.log_list_get_button.grid(
        #     row=1, rowspan=1,
        #     column=0, columnspan=1,
        #     sticky=tkinter.EW,
        # )

        # チェックボックス
        # check_btn_list = tkinter.BooleanVar()
        # check_btn1 = tkinter.Checkbutton(
        #     self, text='1', variable=check_btn_list)
        # check_btn2 = tkinter.Checkbutton(
        #     self, text='2', variable=check_btn_list)

        # check_btn1.pack()
        # check_btn2.pack()

        # print(self.time_from.get())

    def log_view(self, mongo_id):

        record = self.log_get(mongo_id)
        # サブ画面
        log_window = tkinter.Toplevel()
        log_window.title("log view")
        # log_window.geometry("900x500")

        # scrolled_text = scrolledtext.ScrolledText(
        #     log_window, wrap=tkinter.WORD, width=200)
        key_object:list = []
        value_object:list = []
        for idx, (key, value) in enumerate(record.items()):
            # scrolled_text.insert(tkinter.END, str(key) + '\n')  # 1行目のゼロ文字目へ挿入
            key_object.append(tkinter.Entry(log_window,))
            key_object[idx].insert(tkinter.END, str(key))  # 1行目のゼロ文字目へ挿入
            key_object[idx].grid(row=idx, column=0,sticky=tkinter.NW)

            if key == 'logs':
                #scrolled_text.insert(tkinter.END, str(key) + '\n')  # 1行目のゼロ文字目へ挿入

                value_object.append(scrolledtext.ScrolledText(
                    log_window, wrap=tkinter.WORD, width=200))
                value_object[idx].insert(tkinter.END, str(value))  # 1行目のゼロ文字目へ挿入
                value_object[idx].grid(row=idx, column=1)
            else:
                value_object.append(tkinter.Entry(log_window))
                value_object[idx].insert(tkinter.END, str(value))  # 1行目のゼロ文字目へ挿入
                value_object[idx].grid(row=idx, column=1,sticky=tkinter.EW)

        #tree.bind("<<TreeviewSelect>>", lambda:print(''))
        #tree.bind("<<TreeviewSelect>>", lambda:print(''))
        #self.tree.bind("<<TreeviewSelect>>", self.on_tree_select)

    def log_list_get(self, page: int, record_count: int) -> Cursor:
        # 件数制限で順に取得
        limit: int = 20
        skip_list = list(range(0, record_count, limit))

        records: Cursor = self.crawler_log.find(
            filter=self.filter,
            sort=[('start_time', DESCENDING)],
        ).skip(skip_list[page - 1]).limit(limit)

        return records

    def log_get(self, mondo_id: str) -> Any:
        '''ログを１件取得'''
        record = self.crawler_log.find_one(filter={'_id': mondo_id},)
        #print('log_get type ',type(record))
        #print('log_get ',record)
        return record


##################################################
##################################################
##################################################

if __name__ == "__main__":
    root: tkinter.Tk = tkinter.Tk()
    app = LogViewer(root=root)

    root.mainloop()
