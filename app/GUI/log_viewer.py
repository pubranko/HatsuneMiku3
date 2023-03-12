import os
import io
import sys
import re
import pyperclip
import tkinter
import tkinter.font
from tkinter import scrolledtext
from functools import partial
from pydantic import ValidationError
from datetime import datetime
from pprint import pprint
from typing import Any
from pymongo.cursor import Cursor
from pymongo import DESCENDING
path = os.getcwd()
sys.path.append(path)
from shared.directory_search_task import directory_search_task
from shared.timezone_recovery import timezone_recovery
from GUI.log_viewer_validator import LogViewerValidator
from BrownieAtelierMongo.collection_models.mongo_model import MongoModel
from BrownieAtelierMongo.collection_models.crawler_logs_model import CrawlerLogsModel


class LogViewer(tkinter.Frame):
    '''
    crawler_logsコレクションの内容を参照する。
    start_time、record_typeでの絞り込みが可能。
    '''

    def __init__(self, root: tkinter.Tk):
        super().__init__(root, width=1000, height=600, borderwidth=1, relief='groove')
        mongo = MongoModel()
        self.crawler_log = CrawlerLogsModel(mongo)
        # rootウィジェット作成
        root.title('Log Viewer')
        self.root = root
        self.pack()
        self.pack_propagate(False)  # ウィジェットのサイズに合わせてフレームの自動調整を無効化
        # ウィジェット変数定義
        self.current_page = tkinter.IntVar(value=1)
        self.record_count = tkinter.IntVar(value=0)
        self.max_page_count = tkinter.IntVar(value=0)
        self.log_level_value = tkinter.IntVar(value=9)

        self.clipboard_value = tkinter.StringVar()
        #self.master.bind('<Control-v>', self.get_data)
        #self.master.bind('<Control-V>', self.get_data)
        #self.master.bind('<Control-c>', self.set_data)
        #self.master.bind('<Control-C>', self.set_data)

        # その他変数
        self.number_of_lines: int = 10  # 1ページに表示する明細数

        # 初画面作成
        self.init_screen_create()

    def get_data(self, event=None):
        '''クリップボード'''
        try:
            input_string = self.clipboard_get()
            self.clipboard_value.set(input_string)
        except:
            print(sys.exc_info()[1])

    def set_data(self, event=None):
        output_string = self.clipboard_value.get()
        self.clipboard_append(output_string)

    def init_screen_create(self):
        '''
        初画面作成
        '''
        # 初画面表示の項目を定義
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
        #   prefectのTaskディレクトリより取得。
        #   その他個別にログに保管したtypeを追加。    ちょっとこれはあとで見直ししよう、、、直書きはよろしくない
        record_type_list = [str(x['class_name'])
                            for x in directory_search_task()]
        record_type_list.extend(
            ['spider_reports', ])
        # record_type_list.extend(
        #     ['spider_reports', 'news_crawl_async', 'news_clip_master_async', 'solr_news_clip_async', ])
        record_type_list.sort()
        self.record_type_label = tkinter.LabelFrame(
            self, text='record_type')
        # 縦スクロールバー付きのリストボックス
        self.yscroll = tkinter.Scrollbar(
            self.record_type_label, orient=tkinter.VERTICAL)
        self.yscroll.pack(side=tkinter.RIGHT, fill=tkinter.Y)
        self.record_type = tkinter.Listbox(
            self.record_type_label,
            selectmode=tkinter.EXTENDED,                    # リストボックスを複数選択可能とする。
            height=9,                                       # 5行分の高さ
            # 横幅はrecord_type_list内の最長の文字数に合わせる。
            width=max([len(x) for x in record_type_list]),
            yscrollcommand=self.yscroll.set,                # 縦スクロールバーを付与
        )
        # 生成したリストボックスにレコードを挿入
        for rec in record_type_list:
            self.record_type.insert(tkinter.END, rec)
        # レコードタイプを
        self.record_type_label.grid(row=0, rowspan=4, column=1)
        self.record_type.pack()

        # ログメッセージレベルフィルター
        self.log_level_label = tkinter.LabelFrame(
            self, text='ログレベルフィルター')

        self.log_level_none = tkinter.Radiobutton(
            self.log_level_label, text="指定なし", value=9, variable=self.log_level_value)
        self.log_level_none.grid(row=0, column=0, sticky=tkinter.W)
        self.log_level_critical = tkinter.Radiobutton(
            self.log_level_label, text="critical", value=0, variable=self.log_level_value,)
        self.log_level_critical.grid(row=1, column=0, sticky=tkinter.W)
        self.log_level_error = tkinter.Radiobutton(
            self.log_level_label, text="error", value=1, variable=self.log_level_value,)
        self.log_level_error.grid(row=0, column=1, sticky=tkinter.W)
        self.log_level_warning = tkinter.Radiobutton(
            self.log_level_label, text="warning", value=2, variable=self.log_level_value,)
        self.log_level_warning.grid(row=1, column=1, sticky=tkinter.W)

        self.log_level_label.grid(
            row=2, column=0, sticky=tkinter.EW)

        # ログ検索実行ボタン
        self.log_list_get_button = tkinter.Button(
            self, text="ログ一覧取得", command=self.log_list_view)
        self.log_list_get_button.grid(
            row=3, column=0, sticky=tkinter.EW)

        # 情報、ページ関連表示ボックス
        self.info_box = tkinter.Frame(self)
        self.info_box.grid(row=4, column=0, columnspan=2)
        # ラベルとボタンの高さ調整のためフォントを操作
        font1 = tkinter.font.Font(family="Lucida Console", size=15,)
        # 総件数
        self.record_count_label_frame = tkinter.LabelFrame(
            self.info_box, text='総件数')
        self.record_count_label_frame.grid(row=0, column=0)
        self.record_count_label = tkinter.Label(
            self.record_count_label_frame, textvariable=self.record_count, width=6, font=font1)
        self.record_count_label.pack()
        # 総ページ数
        self.page_count_label_frame = tkinter.LabelFrame(
            self.info_box, text='総ページ数')
        self.page_count_label_frame.grid(row=0, column=1)
        self.page_count_label = tkinter.Label(
            self.page_count_label_frame, textvariable=self.max_page_count, width=6, font=font1)
        self.page_count_label.pack()
        # ページセレクター(最初のページ、前ページ、現在のページ、次ページ、最後のページ)
        self.page_selecter_label_frame = tkinter.LabelFrame(
            self.info_box, text='ページ選択', height=2)
        self.page_selecter_label_frame.grid(row=0, column=2, columnspan=8)
        self.first_page_button = tkinter.Button(
            self.page_selecter_label_frame, text='<<', width=2, state='disabled', command=lambda: self.logs_edit('first'))
        self.first_page_button.grid(row=0, column=0)
        self.previous_page_button = tkinter.Button(
            self.page_selecter_label_frame, text='<', width=2, state='disabled', command=lambda: self.logs_edit('-1'))
        self.previous_page_button.grid(row=0, column=1)
        self.current_page_button = tkinter.Button(self.page_selecter_label_frame, text=str(
            self.current_page.get()), width=2, state='disabled', command='')
        self.current_page_button.grid(row=0, column=2)
        self.next_page_button = tkinter.Button(
            self.page_selecter_label_frame, text='>', width=2, state='disabled', command=lambda: self.logs_edit('1'))
        self.next_page_button.grid(row=0, column=3)
        self.last_page_button = tkinter.Button(
            self.page_selecter_label_frame, text='>>', width=2, state='disabled', command=lambda: self.logs_edit('last'))
        self.last_page_button.grid(row=0, column=4)

    def log_list_view(self):
        '''抽出条件を満たすログのリストを作成'''

        try:
            # 検索条件項目のバリデーションを実施
            condition_items = LogViewerValidator(
                date_from=self.date_from.get(),
                time_from=self.time_from.get(),
                date_to=self.date_to.get(),
                time_to=self.time_to.get(),
                record_type=[self.record_type.get(
                    i, i)[0] for i in self.record_type.curselection()],
                log_level_value=self.log_level_value.get(),
            )
            print('検索条件 : ', condition_items.dict())
        except ValidationError as e:
            # print(e.json())  # エラー結果をjson形式で見れる。
            print('エラー内容 : ', e.errors())  # エラー結果をlist形式で見れる。
            # print(str(e))  # エラー結果をlist形式で見れる。
        else:
            # バリデーションでエラーがなかった場合、以下の処理を実施

            # 画面で指定された検索条件より、mongoDB検索用のフィルターを作成
            conditions: list = []
            if condition_items.date_from:
                conditions.append(
                    {'start_time': {'$gte': condition_items.datetime_from()}})
            if condition_items.date_to:
                conditions.append(
                    {'start_time': {'$lte': condition_items.datetime_to()}})
            if condition_items.log_level_value == 9:  # ログレベル指定なしの場合
                if len(condition_items.record_type):
                    conditions.append(
                        {'record_type': {'$in': [condition_items.record_type]}})
            else:
                conditions.append(
                    {'record_type': {'$in': [str(x['class_name']) for x in directory_search_task()]}})

                pattern_traceback = re.compile(
                    r'Traceback \(most recent call last\)\:')
                pattern_critical = re.compile(
                    r'[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2} CRITICAL ')
                pattern_error = re.compile(
                    r'[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2} ERROR ')
                pattern_warning = re.compile(
                    r'[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2} WARNING')
                log_level_selecter: list = []
                if condition_items.log_level_value == 0:
                    log_level_selecter = [
                        pattern_traceback, pattern_critical, ]
                elif condition_items.log_level_value == 1:
                    log_level_selecter = [pattern_traceback,
                                          pattern_critical, pattern_error]
                elif condition_items.log_level_value == 2:
                    log_level_selecter = [pattern_traceback, pattern_critical,
                                          pattern_error, pattern_warning]
                if len(log_level_selecter):
                    conditions.append({'logs': {'$in': log_level_selecter}})

            if conditions:
                self.filter: Any = {'$and': conditions}
            else:
                self.filter = None

            print('mongoDB検索フィルター : ', self.filter)

            # 検索対象の件数、ページ数を確認
            self.record_count.set(self.crawler_log.count(filter=self.filter))
            # 小数点以下切り上げ
            self.max_page_count.set(-(-self.record_count.get() //
                                    self.number_of_lines))

            # 該当のログがある場合
            if self.record_count.get():

                # ログリスト表示フレーム作成
                self.logs_frame = tkinter.Frame(
                    self, relief="ridge", bd=3, padx=5, pady=5,)
                self.logs_frame.grid(row=5, column=0, columnspan=4,
                                     sticky=tkinter.W)

                # ログリストの見出しと行数分の空明細を作成。
                self.logs_table: list = []
                # 見出し
                self.logs_table.append([
                    tkinter.Label(self.logs_frame, text='No.',
                                  relief='groove', width=4, ),
                    tkinter.Label(self.logs_frame, text='mongo_id',
                                  relief='groove', width=25),
                    tkinter.Label(self.logs_frame, text='record_type',
                                  relief='groove', width=25),
                    tkinter.Label(self.logs_frame, text='domain',
                                  relief='groove', width=15),
                    tkinter.Label(self.logs_frame, text='start_time',
                                  relief='groove', width=20),
                    tkinter.Label(self.logs_frame, text='', relief='groove', width=5), ])  # 表示ボタン
                # 明細
                idx = 1
                while idx <= self.number_of_lines:
                    self.logs_table.append([
                        tkinter.Label(self.logs_frame,
                                      text='', relief='ridge',),
                        tkinter.Label(self.logs_frame,
                                      text='', relief='ridge', anchor="w"),
                        tkinter.Label(self.logs_frame,
                                      text='', relief='ridge', anchor="w"),
                        tkinter.Label(self.logs_frame,
                                      text='', relief='ridge', anchor="w"),
                        tkinter.Label(self.logs_frame,
                                      text='', relief='ridge', anchor="w"),
                        tkinter.Button(self.logs_frame,
                                       text='', pady=0, command='', width=5)])
                    idx += 1
                # 初回は1ページ目分より開始
                self.logs_edit(page_adjustment='first')
            else:
                # 該当レコードがない場合は明細欄をクリア
                idx = 1
                while idx <= self.number_of_lines:
                    self.logs_table[idx][0]['text'] = ''
                    self.logs_table[idx][1]['text'] = ''
                    self.logs_table[idx][2]['text'] = ''
                    self.logs_table[idx][3]['text'] = ''
                    self.logs_table[idx][4]['text'] = ''
                    self.logs_table[idx][5]['text'] = ''
                    self.logs_table[idx][5]['command'] = ''

                    idx += 1

    def logs_edit(self, page_adjustment: str):
        ''' '''
        # ページ制御
        self.pagenate(page_adjustment)

        # 上記で指定したページのログを取得
        records,records_count = self.logs_list_get(
            self.current_page.get(), self.record_count.get())

        # 上記で取得したログを明細ウィジェットへ編集
        self.logs_detail_edit(self.logs_frame, self.logs_table, records, records_count)

    def pagenate(self, page_adjustment):
        '''
        現在のページから他ページボタンの活性・非活性をコントロール
        '''
        if page_adjustment == 'first':
            current_page: int = 1
        elif page_adjustment == 'last':
            current_page: int = self.max_page_count.get()
        else:
            current_page: int = self.current_page.get() + int(page_adjustment)
        self.current_page.set(current_page)

        if current_page <= 1:
            # 次ページ、最後のページを活性化
            self.first_page_button['state'] = 'disabled'
            self.previous_page_button['state'] = 'disabled'
        else:
            self.first_page_button['state'] = 'normal'
            self.previous_page_button['state'] = 'normal'

        self.current_page_button['text'] = current_page

        if current_page >= self.max_page_count.get():
            self.next_page_button['state'] = 'disabled'
            self.last_page_button['state'] = 'disabled'
        else:
            self.next_page_button['state'] = 'normal'
            self.last_page_button['state'] = 'normal'

    def logs_detail_edit(self, logs_frame, logs_table, records: Cursor, records_count:int):
        '''
        各レコードをログ明細エリアへ編集
        '''
        for idx, record in enumerate(records):

            # レコードよりウィジェットへ設定
            self.logs_table[idx + 1][0]['text'] = \
                str(idx + 1 + ((self.current_page.get() - 1) * self.number_of_lines))
            self.logs_table[idx + 1][1]['text'] = record['_id']
            self.logs_table[idx + 1][2]['text'] = record['record_type']
            self.logs_table[idx + 1][3]['text'] = \
                record['domain'] if 'domain' in record else ''
            start_time: datetime = timezone_recovery(record['start_time'])
            self.logs_table[idx + 1][4]['text'] = \
                start_time.strftime('%Y-%m-%d %H:%M:%S %Z')
            self.logs_table[idx + 1][5]['text'] = '詳細表示'
            self.logs_table[idx + 1][5]['command'] = \
                partial(self.log_view, record['_id'])

            # ログにCRITICAL、ERROR、WARNINGが含まれていた場合、文字色を変える。
            text_color: str = '#000000'  # 黒
            if 'logs' in record:
                # 改行含む場合
                #raise Exception
                #CRITICAL > ERROR > WARNING > INFO > DEBUG
                # 2021-08-08 12:31:04 [scrapy.core.engine] INFO: Spider closed (finished)
                # クリティカルの場合、ログ形式とは限らない。raiseなどは別形式のため、後日検討要。
                # Traceback (most recent call last):
                pattern_traceback = re.compile(
                    r'Traceback \(most recent call last\)\:')
                pattern_critical = re.compile(
                    r'[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2} CRITICAL ')
                pattern_error = re.compile(
                    r'[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2} ERROR ')
                pattern_warning = re.compile(
                    r'[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2} WARNING')

                if pattern_traceback.search(record['logs']):
                    text_color: str = '#FF0000'  # 赤
                elif pattern_critical.search(record['logs']):
                    text_color: str = '#FF0000'  # 赤
                elif pattern_error.search(record['logs']):
                    text_color: str = '#800000'  # 茶色
                elif pattern_warning.search(record['logs']):
                    text_color: str = '#FF9933'  # オレンジ

            self.logs_table[idx + 1][0]['fg'] = text_color
            self.logs_table[idx + 1][1]['fg'] = text_color
            self.logs_table[idx + 1][2]['fg'] = text_color
            self.logs_table[idx + 1][3]['fg'] = text_color
            self.logs_table[idx + 1][4]['fg'] = text_color
            self.logs_table[idx + 1][5]['fg'] = text_color

        # 余った明細エリアは空欄で埋める。
        # あまりの明細がなければ何もしない。
        amari = self.current_page.get() * self.number_of_lines - records_count
        idx = self.number_of_lines + 1
        if amari > 0:
            idx = self.number_of_lines - 1
            while idx >= self.number_of_lines - amari:
                self.logs_table[idx + 1][0]['text'] = ''
                self.logs_table[idx + 1][1]['text'] = ''
                self.logs_table[idx + 1][2]['text'] = ''
                self.logs_table[idx + 1][3]['text'] = ''
                self.logs_table[idx + 1][4]['text'] = ''
                self.logs_table[idx + 1][5]['text'] = ''
                self.logs_table[idx + 1][5]['command'] = ''

                idx -= 1

        # logs_tableよりログリスト表示エリアの明細へ設定
        for row_idx, col_items in enumerate(logs_table):
            for col_idx, col_item in enumerate(col_items):
                col_item.grid(row=row_idx, column=col_idx,
                              sticky=tkinter.EW)

    def log_view(self, mongo_id):
        '''ログの詳細を表示'''
        # ログを取得
        record = self.log_get(mongo_id)
        # ログ表示用サブ画面作成
        log_window = tkinter.Toplevel()
        log_window.title(f"log view({mongo_id})")
        # 各項目の行の挿入位置のポインター:初期化
        row_pointer = 0
        for record_key, record_value in record.items():
            # 項目名、コピーボタン
            item_name = tkinter.Entry(log_window,)
            item_name.insert(tkinter.END, str(record_key))
            item_name.grid(row=row_pointer, column=0,
                           sticky=tkinter.NW, ipadx=30)
            copy_button = tkinter.Button(log_window, text="コピー", command="",)

            # valueを種類に応じて表示
            if record_key == 'logs':
                # 項目名、コピーボタン
                copy_button.grid(row=row_pointer + 1, column=0,
                                 sticky=tkinter.NW, ipadx=30)
                copy_button['command'] = lambda: pyperclip.copy(
                    str(record_value))
                # 項目値
                item_value = scrolledtext.ScrolledText(
                    log_window, wrap=tkinter.WORD, width=200)
                item_value.insert(tkinter.END, str(record_value))
                item_value.grid(row=row_pointer, rowspan=2, column=1)
                # 次の行ポインターへ
                row_pointer += 2
            elif record_key in ['stats', 'crawl_urls_list']:
                # 文字列を改行付きに整形
                file_like = io.StringIO('')
                pprint(record_value, stream=file_like)
                # 項目名、コピーボタン
                copy_button.grid(row=row_pointer + 1, column=0,
                                 sticky=tkinter.NW, ipadx=30)
                copy_button['command'] = self.clipboard_copy(
                    file_like.getvalue())
                # 項目値
                item_value = scrolledtext.ScrolledText(
                    log_window, wrap=tkinter.WORD, width=200, height=self.number_of_lines)
                item_value.insert(tkinter.END, file_like.getvalue())
                item_value.grid(row=row_pointer, rowspan=2, column=1)
                # 次の行ポインターへ
                row_pointer += 2
            else:
                # 項目値
                item_value = tkinter.Entry(log_window)
                item_value.insert(tkinter.END, str(record_value))
                item_value.grid(row=row_pointer, column=1,
                                sticky=tkinter.EW, ipadx=500)
                # 次の行ポインターへ
                row_pointer += 1

    def logs_list_get(self, page: int, record_count: int) -> tuple[Cursor,int]:
        '''指定されたページに表示する分のログを取得して返す'''
        limit: int = self.number_of_lines
        skip_list = list(range(0, record_count, limit))

        record_count = self.crawler_log.count(filter=self.filter)
        record = self.crawler_log.find(
            filter=self.filter,
            sort=[('start_time', DESCENDING)],
        ).skip(skip_list[page - 1]).limit(limit)

        return record,record_count

    def log_get(self, mondo_id: str) -> Any:
        '''ログを１件取得'''
        return self.crawler_log.find_one(filter={'_id': mondo_id},)

    def clipboard_copy(self, record_value_shaping: str):
        '''
        対象のウィジェットのテキストをクリップボードへコピーする。
        ※これは少々ややこしかった。以下のサイトを参考にした。
          https://memopy.hatenadiary.jp/entry/2017/06/11/220452
        '''
        def a(record_value_shaping):
            pyperclip.copy(record_value_shaping)
        return lambda: a(record_value_shaping)


##################################################
if __name__ == "__main__":
    root: tkinter.Tk = tkinter.Tk()
    app = LogViewer(root=root)

    root.mainloop()
