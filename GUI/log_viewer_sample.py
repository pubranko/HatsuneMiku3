import os
import sys
import tkinter
from tkinter import ttk
from tkinter.constants import ANCHOR, CENTER, COMMAND
import tkcalendar
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
            self, text='record_type'
        )
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
            sticky=tkinter.EW,
        )

    def log_list_get(self, page: int, record_count: int) -> Cursor:
        # 件数制限で順に取得
        limit: int = 10
        skip_list = list(range(0, record_count, limit))

        records: Cursor = self.crawler_log.find(
            filter=self.filter,
            sort=[('start_time', DESCENDING)],
        ).skip(skip_list[page - 1]).limit(limit)

        return records

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

        # サブ画面
        logs_window = tkinter.Toplevel()
        logs_window.title("sub window")
        logs_window.geometry("900x500")

        # Treeviewの生成
        column = ('No.', 'record_type', 'domain')
        self.tree = ttk.Treeview(
            logs_window, columns=column,
            #selectmode='browse'
            )
        # 列の設定を定義
        self.tree.column('#0', width=0, stretch=False)
        self.tree.column('No.', anchor='center', width=40)
        self.tree.column('record_type', anchor='w', width=100)
        self.tree.column('domain', anchor='center', width=80)
        # 列の見出し設定
        self.tree.heading('#0', text='')
        self.tree.heading('No.', text='No.')
        self.tree.heading('record_type', text='record_type')
        self.tree.heading('domain', text='domain')

        for idx, record in enumerate(records):
            # レコードの追加
            self.tree.insert(parent='', index='end', iid=str(idx), values=(
                idx + 1,
                record['record_type'],
                record['domain'] if 'domain' in record else '',
            ))

        self.tree.grid(row=0, column=0)
        #tree.bind("<<TreeviewSelect>>", lambda:print(''))
        #tree.bind("<<TreeviewSelect>>", lambda:print(''))
        self.tree.bind("<<TreeviewSelect>>", self.on_tree_select)

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
    def on_tree_select(self, event):
        print("selected items:", self.tree.selection()[0])
        #for item in self.tree.selection():
        #    print(item)

##################################################
##################################################
##################################################


root: tkinter.Tk = tkinter.Tk()
app = LogViewer(root=root)
# root.geometry('500x300')

# log_list_get_button = tkinter.Button(root, text='ログ一覧表示')
# log_list_get_button.pack()

# log_lists = tkinter.Listbox()

# サブウィンドウの作成
# subwindow = tkinter.Toplevel()
# subwindow.title('Sub window')
# subwindow.config(bg='#123123')
# subwindow.geometry('200x300+500+500')


root.mainloop()
