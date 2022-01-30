import japanize_kivy
from kivy.config import Config
from kivy.app import App
from kivy.uix.widget import Widget
from kivy.properties import StringProperty
from kivy.core.text import LabelBase, DEFAULT_FONT
from kivy.resources import resource_add_path
from kivy.uix.label import Label
from kivy.metrics import dp

Config.set('graphics', 'width', '640')
Config.set('graphics', 'height', '480')
# デフォルトに使用するフォントを変更する
#resource_add_path('/System/Library/Fonts')
#LabelBase.register(DEFAULT_FONT, 'PingFang.ttc') #日本語が使用できるように日本語フォントを指定する


class DateTimeBox(Widget):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
class DateTimeLabel(Widget):
    pass

class LogSelecter(Widget):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def buttonStarted(self):
        #self.source= './image/000001.jpg'
        pass

    def buttonRandom(self):
        #self.source= './image/000001.jpg'
        pass

class LogViewerApp(App):    #この名前が、同一ディレクトリにあるlogviewer.kvのファイルとリンクしている。appクラス内でやっている。
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.title = 'Log Viewer'
    def build(self):
        self.label = Label(size_hint = (None, None))
        self.label.bind(size = self.label.setter('text_size'))
        self.label.text = 'ハローワールド' # テキスト
        self.label.font_size = dp(16) # フォントサイズ
        self.label.bold = False # 太字
        self.label.italic = False # イタリック
        self.label.color = (1,1,1,1) # 文字色
        self.label.pos = (dp(0), dp(240-24)) # 位置
        self.label.size = (dp(240), dp(24)) # サイズ
        self.label.halign = "left"  # 水平揃え
        self.label.valign = "middle" # 垂直揃え
        self.root.add_widget(self.label)

if __name__ == '__main__':
    LogViewerApp().run()
