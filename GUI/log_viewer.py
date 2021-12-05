import tkinter as tk

root = tk.Tk()
root.title('Log Viewer')
root.geometry('500x300')

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

log_list_get_button = tk.Button(root, text = 'ログ一覧表示')
log_list_get_button.pack()

log_lists = tk.Listbox()

# サブウィンドウの作成
# subwindow = tkinter.Toplevel()
# subwindow.title('Sub window')
# subwindow.config(bg='#123123')
# subwindow.geometry('200x300+500+500')

root.mainloop()