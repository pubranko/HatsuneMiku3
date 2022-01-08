import japanize_kivy
from kivy.config import Config
from kivy.app import App
from kivy.uix.widget import Widget
from kivy.properties import StringProperty
from kivy.core.text import LabelBase, DEFAULT_FONT
from kivy.resources import resource_add_path

Config.set('graphics', 'width', '640')
Config.set('graphics', 'height', '480')
# デフォルトに使用するフォントを変更する
#resource_add_path('/System/Library/Fonts')
#LabelBase.register(DEFAULT_FONT, 'PingFang.ttc') #日本語が使用できるように日本語フォントを指定する


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

if __name__ == '__main__':
    LogViewerApp().run()
