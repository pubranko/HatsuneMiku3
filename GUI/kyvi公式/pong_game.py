from kivy.app import App
from kivy.uix.widget import Widget
from kivy.properties import (
    NumericProperty, ReferenceListProperty, ObjectProperty
)
from kivy.vector import Vector
from kivy.clock import Clock
from random import randint

class PongPaddle(Widget):
    '''各プレイヤーのパドルの定義'''
    score = NumericProperty(0)  #得点のカウンターエリア。初期値はゼロ。

    def bounce_ball(self, ball):
        '''
        ＜Widgetのcollide_widgetメソッド＞
            他のウィジェットがこのウィジェットと衝突しているかどうかをチェックします。
            この関数は、デフォルトで軸合わせされたバウンディングボックスの交差テストを実行します。
            引数 = Widgetクラスを渡す
            戻り値 = ブール値。他のウィジェットがこのウィジェットと衝突した場合は True、そうでない場合は False を返します。
        ＜パドルにボールがあたった場合の動作の定義＞
            ・ボールの横の移動方向反転
            ・速度１０％アップ
            ・パドルの中心から離れた距離に応じて縦方向の速度をアップ
        '''
        if self.collide_widget(ball):
            vx, vy = ball.velocity  #ボールの進行方向を取得？
            offset = (ball.center_y - self.center_y) / (self.height / 2)    #(ボールの中心座標(y) - バドルの中心座標(y))  ／  (パドルの高さ/2)
            bounced = Vector(-1 * vx, vy)   #ボールの横の進行方向を反転
            vel = bounced * 1.1             #ボールの移動速度を10%アップ
            ball.velocity = vel.x, vel.y + offset
            #ball.velocity = vel.x, vel.y


class PongBall(Widget):

    # velocity of the ball on x and y axis
    velocity_x = NumericProperty(0) #ボールのx軸への進行速度
    velocity_y = NumericProperty(0) #ボールのy軸への進行速度

    # referencelist property so we can use ball.velocity as
    # a shorthand, just like e.g. w.pos for w.x and w.y
    velocity = ReferenceListProperty(velocity_x, velocity_y)

    # ``move`` function will move the ball one step. This
    #  will be called in equal intervals to animate the ball
    # 最初「self.pos + Vector(*self.velocity)」でやったらエラーとなった。(pos => length is immutable)
    def move(self):
        self.pos = Vector(*self.velocity) + self.pos


class PongGame(Widget):
    '''Pongアプリの大枠のウィンドウと各ウィジェットの配置'''
    ball = ObjectProperty(None) #kvルールで作成されたウィジェットに接続。
    player1 = ObjectProperty(None)
    player2 = ObjectProperty(None)


    def serve_ball(self, vel=(4, 0)):   #初回のボールの進行方向を定義。Vector(x軸への移動速度, y軸への移動速度)に対して、ランダム(0-360度)で角度を与えている。
        self.ball.center = self.center  #ボールの位置？無くても同じ動きに見える、、、
        self.ball.velocity = Vector(4, 0).rotate(randint(0, 360))


    def update(self, dt):
        self.ball.move()

        # bounce of paddles
        # 各プレイヤーのパドルとボールがあたった場合、ボールの挙動を変更している。
        self.player1.bounce_ball(self.ball)
        self.player2.bounce_ball(self.ball)

        # bounce off top and bottom 上下の枠を超えたら、ボールの進行方向（縦）を反転
        if (self.ball.y < 0) or (self.ball.top > self.height):
            self.ball.velocity_y *= -1

        # bounce off left and right 左右の枠を超えたら、ボールの進行方向（横）を反転
        #if (self.ball.x < 0) or (self.ball.right > self.width):
        #    self.ball.velocity_x *= -1

        # went of to a side to score point?
        # ボールのx軸が、左枠を超えた場合、プレイヤー２へ加点し、再度中心からボールをサーブする。
        if self.ball.x < self.x:
            self.player2.score += 1
            self.serve_ball(vel=(4, 0))
        # ボールのx軸が、右枠を超えた場合、プレイヤー１へ加点し、再度中心からボールをサーブする。
        if self.ball.x > self.width:
            self.player1.score += 1
            self.serve_ball(vel=(-4, 0))

    def on_touch_move(self, touch):
        '''
        Widgetクラスのオーバーライドっぽい。
        他にもon_touch_down(タッチが発生)、on_touch_up（タッチがなくなった）、on_kv_post（よくわからん）がある。
        on_touch_moveは、既存のタッチが移動したときに発行されます。
        マウス操作やタップなどのタッチした情報がオブジェクトとしてtouchに渡されてくる。
        '''
        #ゲーム画面の左３分の１をタッチされた場合
        #プレイヤー１のパドルを移動する。
        if touch.x < self.width / 3:
            self.player1.center_y = touch.y

        #ゲーム画面の右３分の１をタッチされた場合
        #プレイヤー２のパドルを移動する。
        if touch.x > self.width - self.width / 3:
            self.player2.center_y = touch.y

class PongApp(App):
    '''Pongアプリの本体'''
    def build(self):
        game = PongGame()
        game.serve_ball()   #初期のボールの移動方向をランダムで指定、速度は4
        Clock.schedule_interval(game.update, 1.0 / 60.0)    #1/60秒間隔(60FPS)でgame.updateを呼び出し
        return game


if __name__ == '__main__':
    PongApp().run()
