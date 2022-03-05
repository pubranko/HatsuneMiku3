from datetime import datetime
import pandas as pd


class StatsInfoCollectData:
    '''
    スパイダーごとに１日分のデータの集計を行う。
    ・入力データを１けんづつpandasのdataframeに格納する。
    '''
    # [spider1の集計結果、spider2の集計結果]

    # ログ１件より生成するmondoDBへ保存するデータイメージ。基準日とスパイダー名がkeyになる。
    # データフレームの列イメージ
    # 開始時間    スパイダー名、ログレベルカウント            ,処理時間           ,メモリ使用量 リクエスト数              レスポンス数
    # start_time,spider_name,log_count/CRITICAL,ERROR,WARNING,elapsed_time_seconds,memusage/max,downloader/request_count,downloader/response_count,
    # レスポンスのエラー件数
    # robotstxt/response_status_count/＊、downloader/response_status_count/＊
    # リクエストの深さ(最大)  レスポンスのバイト数       ,リトライ件数   ,保存した件数       ,終了理由
    #request_depth_max      ,downloader/response_bytes  ,retry/count    ,item_scraped_count ,finish_reason

    #spider_stats_totalization_data: dict = field(default_factory=dict)
    '''データイメージ(domain = sankei.com + ロイター の例)'''

    robots_response_status_dataframe: pd.DataFrame
    downloader_response_status_dataframe: pd.DataFrame
    spider_stats_datafram: pd.DataFrame

    def __init__(self):

        self.robots_response_status_dataframe = pd.DataFrame({
            'record_type': [],
            'start_time': [],
            'time_period_hour':[],
            'spider_name': [],
            'robots_response_status': [],
            'count': [],
        })

        self.downloader_response_status_dataframe = pd.DataFrame({
            'record_type': [],
            'start_time': [],
            'time_period_hour':[],
            'spider_name': [],
            'downloader_response_status': [],
            'count': [],
        })

        self.spider_stats_datafram = pd.DataFrame({
            'record_type': [],
            'start_time': [],
            'time_period_hour':[],
            'spider_name': [],
            'log_count/CRITICAL': [],
            'log_count/ERROR': [],
            'log_count/WARNING': [],
            'elapsed_time_seconds': [],
            'memusage/max': [],
            'downloader/request_count': [],
            'downloader/response_count': [],
            'request_depth_max': [],
            'downloader/response_bytes': [],
            'retry/count': [],
            'item_scraped_count': [],
            'finish_reason': [],
        })

    def spider_stats_store(self, start_time: datetime, spider_name: str, stats: dict,) -> None:
        '''pandasのデータフレームを作成する。'''

        for key, value in stats.items():
            # 例）robotstxt/response_status_count/403、robotstxt/response_status_count/200 など
            # 例）downloader/response_status_count/200、downloader/response_status_count/404 など
            if 'robotstxt/response_status_count/' in key:
                self.robots_response_status_dataframe = self.robots_response_status_dataframe.append({
                    'record_type': 'robots_response_status',
                    'start_time': start_time,
                    'time_period_hour':start_time.strftime('%H'),
                    'spider_name': spider_name,
                    'robots_response_status': str(key).replace('robotstxt/response_status_count/', ''),
                    'count': value,
                }, ignore_index=True)
            if 'downloader/response_status_count/' in key:
                self.downloader_response_status_dataframe = self.downloader_response_status_dataframe.append({
                    'record_type': 'downloader_response_status',
                    'start_time': start_time,
                    'time_period_hour':start_time.strftime('%H'),
                    'spider_name': spider_name,
                    'downloader_response_status': str(key).replace('downloader/response_status_count/', ''),
                    'count': value,
                }, ignore_index=True)

        self.spider_stats_datafram = self.spider_stats_datafram.append({
            'record_type': 'spider_stats',
            'start_time': start_time,
            'time_period_hour':start_time.strftime('%H'),
            'spider_name': spider_name,
            'log_count/CRITICAL': stats['log_count/CRITICAL'] if 'log_count/CRITICAL' in stats else 0,
            'log_count/ERROR': stats['log_count/ERROR'] if 'log_count/ERROR' in stats else 0,
            'log_count/WARNING': stats['log_count/WARNING'] if 'log_count/WARNING' in stats else 0,
            'elapsed_time_seconds': stats['elapsed_time_seconds'] if 'elapsed_time_seconds' in stats else 0,
            'memusage/max': stats['memusage/max'] if 'memusage/max' in stats else 0,
            'downloader/request_count': stats['downloader/request_count'] if 'downloader/request_count' in stats else 0,
            'downloader/response_count': stats['downloader/response_count'] if 'downloader/response_count' in stats else 0,
            'request_depth_max': stats['request_depth_max'] if 'request_depth_max' in stats else 0,
            'downloader/response_bytes': stats['downloader/response_bytes'] if 'downloader/response_bytes' in stats else 0,
            'retry/count': stats['retry/count'] if 'retry/count' in stats else 0,
            'item_scraped_count': stats['item_scraped_count'] if 'item_scraped_count' in stats else 0,
            'finish_reason': stats['finish_reason'] if 'finish_reason' in stats else 0,
        }, ignore_index=True)

    # ログ１件より生成するデータイメージ。基準日とスパイダー名がkeyになる。
    # temp = {
    #     'start_time': "datetime形式",
    #     'spider_name': "名前",
    #     'number_of_times_executed': "実行回数",
    #     'totalization': {
    #         # # ログレベル件数
    #         #     ログレベル(CRITICAL) log_count/CRITICAL
    #         #     ログレベル(ERROR)    log_count/ERROR
    #         #     ログレベル(WARNING)  log_count/WARNING
    #         # # elapsed_time_secondsを使う
    #         #     処理時間(最小)
    #         #     処理時間(最大)
    #         #     処理時間(合計)
    #         #     処理時間(平均)
    #         # # メモリ使用量  memusage/maxを使う
    #         #     メモリ使用量(最小)
    #         #     メモリ使用量(最大)
    #         #     メモリ使用量(平均)
    #         # # 総リクエスト数、総レスポンス数(downloader/request_count,downloader/response_count)
    #         #     総リクエスト数(最小)
    #         #     総リクエスト数(最大)
    #         #     総リクエスト数(合計)
    #         #     総リクエスト数(平均)
    #         #     総レスポンス数(最小)
    #         #     総レスポンス数(最大)
    #         #     総レスポンス数(合計)
    #         #     総レスポンス数(平均)
    #         # # レスポンスのエラー件数（200以外の数をカウント）robotstxt/response_status_count/＊、downloader/response_status_count/＊
    #         #     robotsレスポンスのエラー件数(平均)
    #         #     robotsレスポンスのエラー件数(合計)
    #         #     レスポンスのエラー件数(平均)
    #         #     レスポンスのエラー件数(合計)
    #         # # リクエストの深さ(最大)
    #         #     リクエスト最大深度  request_depth_maxの最大値
    #         # # レスポンスのバイト数 downloader/response_bytes
    #         #     レスポンスバイト数(平均)
    #         #     レスポンスバイト数(合計)
    #         # # リトライ件数 retry/count
    #         #     リトライ件数(平均)
    #         #     リトライ件数(合計)
    #         # # 保存した件数 item_scraped_count
    #         #     平均
    #         #     合計
    #         # # 異常終了ステータス    finish_reason: "finished"
    #         #     finish_reasonが"finished"以外の件数
    #     }, }

    # stats = {
    #     # ログレベル件数
    #     "log_count/CRITICAL": 0,
    #     "log_count/ERROR": 0,
    #     "log_count/WARNING": 1,
    #     "log_count/INFO": 31,
    #     "log_count/DEBUG": 39,
    #     # 開始〜終了までの時間
    #     # ISODate("2021-11-28T13:20:00.120Z"),
    #     "start_time": "2021-11-28T13:20:00.120Z",
    #     # ISODate("2021-11-28T13:21:10.791Z"),
    #     "finish_time": "2021-11-28T13:21:10.791Z",
    #     # 経過時間
    #     "elapsed_time_seconds": 70.670898,
    #     # メモリ使用量
    #     "memusage/startup": 145358848,
    #     "memusage/max": 153153536,
    #     # スケジューラーの待受け、処理待ち、、、これはなんだ？
    #     "scheduler/enqueued/memory": 34,
    #     "scheduler/enqueued": 34,
    #     "scheduler/dequeued/memory": 34,
    #     "scheduler/dequeued": 34,
    #     # 総リクエスト数、総レスポンス数
    #     "downloader/request_count": 22,
    #     "downloader/response_count": 21,
    #     "response_received_count": 21,
    #     # リクエスト内訳
    #     "downloader/request_method_count/GET": 6,
    #     "downloader/request_method_count/POST": 16,
    #     # レスポンス内訳
    #     "downloader/response_status_count/200": 19,
    #     "downloader/response_status_count/403": 1,
    #     "downloader/response_status_count/404": 1,
    #     # robotsのリクエスト、レスポンス内訳
    #     "robotstxt/request_count": 3,
    #     "robotstxt/response_count": 3,
    #     "robotstxt/response_status_count/200": 1,
    #     "robotstxt/response_status_count/403": 1,
    #     "robotstxt/response_status_count/404": 1,
    #     # リクエストの深さ別カウント
    #     "request_depth_count/0": 1,
    #     "request_depth_count/1": 2,
    #     "request_depth_count/2": 15,
    #     "request_depth_max": 2,
    #     # データ容量
    #     "downloader/request_bytes": 14665,
    #     "downloader/response_bytes": 7222191,
    #     # 圧縮カウント、バイト
    #     "httpcompression/response_count": 3,
    #     "httpcompression/response_bytes": 16533,
    #     # ダウンロードの異常終了カウント、タイプカウント
    #     "downloader/exception_count": 1,
    #     "downloader/exception_type_count/twisted_web__newclient_ResponseNeverReceived": 1,
    #     # スプラッシュ
    #     "splash/render_html/request_count": 15,
    #     "splash/render_html/response_count/200": 15,
    #     "splash/execute/request_count": 3,
    #     "splash/execute/response_count/200": 3,
    #     # リトライカウント
    #     "retry/count": 1,
    #     "retry/reason_count/twisted_web__newclient_ResponseNeverReceived": 1,
    #     # アイテムパイプラインで保存した件数
    #     "item_scraped_count": 15,
    #     # 終了理由
    #     "finish_reason": "finished"
    # }
