from __future__ import annotations
from datetime import datetime
from typing import Any
import pandas as pd
from copy import deepcopy
import itertools


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

    robots_df: pd.DataFrame
    downloader_df: pd.DataFrame
    spider_df: pd.DataFrame

    #{'sum': df, 'mean': df, 'min': df, 'max': df}
    robots_result_df: dict[str, pd.DataFrame]
    downloader_result_df: dict[str, pd.DataFrame]
    spider_result_df: dict[str, pd.DataFrame]

    def __init__(self):
        self.robots_df = pd.DataFrame({
            'record_type': [],
            'start_time': [],
            'time_period_hour': [],
            'spider_name': [],
            'robots_response_status': [],
            'count': [],
        })
        self.downloader_df = pd.DataFrame({
            'record_type': [],
            'start_time': [],
            'time_period_hour': [],
            'spider_name': [],
            'downloader_response_status': [],
            'count': [],
        })
        self.spider_df = pd.DataFrame({
            'record_type': [],
            'start_time': [],
            'time_period_hour': [],
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

        robots_result_col = {
            'aggregate_base_term': [],
            'spider_name': [],
            'robots_response_status': [],
            'count': [],
        }
        downloader_result_col = {
            'aggregate_base_term': [],
            'spider_name': [],
            'downloader_response_status': [],
            'count': [],
        }
        spider_result_col = {
            'aggregate_base_term': [],
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
        }

        #{'sum': df, 'mean': df, 'min': df, 'max': df}
        self.robots_result_df: dict[str, pd.DataFrame] = {
            'sum': pd.DataFrame(robots_result_col),
            'mean': pd.DataFrame(robots_result_col),
            'min': pd.DataFrame(robots_result_col),
            'max': pd.DataFrame(robots_result_col),
        }
        self.downloader_result_df: dict[str, pd.DataFrame] = {
            'sum': pd.DataFrame(downloader_result_col),
            'mean': pd.DataFrame(downloader_result_col),
            'min': pd.DataFrame(downloader_result_col),
            'max': pd.DataFrame(downloader_result_col),
        }
        self.spider_result_df: dict[str, pd.DataFrame] = {
            'sum': pd.DataFrame(spider_result_col),
            'mean': pd.DataFrame(spider_result_col),
            'min': pd.DataFrame(spider_result_col),
            'max': pd.DataFrame(spider_result_col),
        }

    def spider_stats_store(self, start_time: datetime, spider_name: str, stats: dict,) -> None:
        '''
        pandasのデータフレームを作成する。
        ※pandasのappendは処理効率が悪い。後日処理方法を見直し予定。
        '''
        for key, value in stats.items():
            # 例）robotstxt/response_status_count/403、robotstxt/response_status_count/200 など
            # 例）downloader/response_status_count/200、downloader/response_status_count/404 など
            if 'robotstxt/response_status_count/' in key:
                self.robots_df = self.robots_df.append({
                    'record_type': 'robots_response_status',
                    'start_time': start_time,
                    'time_period_hour': start_time.strftime('%H'),
                    'spider_name': spider_name,
                    'robots_response_status': str(key).replace('robotstxt/response_status_count/', ''),
                    'count': value,
                }, ignore_index=True)
            if 'downloader/response_status_count/' in key:
                self.downloader_df = self.downloader_df.append({
                    'record_type': 'downloader_response_status',
                    'start_time': start_time,
                    'time_period_hour': start_time.strftime('%H'),
                    'spider_name': spider_name,
                    'downloader_response_status': str(key).replace('downloader/response_status_count/', ''),
                    'count': value,
                }, ignore_index=True)

        self.spider_df = self.spider_df.append({
            'record_type': 'spider_stats',
            'start_time': start_time,
            'time_period_hour': start_time.strftime('%H'),
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

    def dataframe_recovery(self, stats_info_collect_record: dict) -> None:
        '''引数のレコードを基にpandasのデータフレームを復元する。'''
        if stats_info_collect_record['record_type'] == 'robots_response_status':
            self.robots_df = self.robots_df.append(
                stats_info_collect_record, ignore_index=True)
        elif stats_info_collect_record['record_type'] == 'downloader_response_status':
            self.downloader_df = self.downloader_df.append(
                stats_info_collect_record, ignore_index=True)
        elif stats_info_collect_record['record_type'] == 'spider_stats':
            self.spider_df = self.spider_df.append(
                stats_info_collect_record, ignore_index=True)

    def date_time_set_index(self, columns: str, df: pd.DataFrame):
        '''
        引数で指定されたデータフレームに対し、引数で指定したカラムの
        インデックスを追加したデータフレームを返す。
        '''
        df[columns] = pd.to_datetime(df[columns])
        df.set_index(['start_time']).tz_localize(
            'UTC').tz_convert('Asia/Tokyo')
        return df.set_index(['start_time']).tz_localize('UTC').tz_convert('Asia/Tokyo')

    def stats_analysis_exec(self, datetime_term_list: list[tuple[datetime, datetime]]) -> pd.DataFrame:
        '''引数で渡された集計期間リストごとに解析を実行'''
        # start_timeをインデックスとしたdataframを生成
        # index生成後ソートを実行する。※ソートしないと将来的にエラーになると警告を受ける。
        robots_df_index = self.date_time_set_index(
            'start_time', self.robots_df).sort_index()
        downloader_df_index = self.date_time_set_index(
            'start_time', self.downloader_df).sort_index()
        spider_df_index = self.date_time_set_index(
            'start_time', self.spider_df).sort_index()

        date_list: list = []
        for calc_date_from, calc_date_to in datetime_term_list:
            # 集計期間ごとに抽出
            date_from = calc_date_from.strftime('%Y-%m-%d %H:%M:%S.%f')
            date_to = calc_date_to.strftime('%Y-%m-%d %H:%M:%S.%f')
            robots_select_df = robots_df_index[date_from: date_to]
            downloader_select_df = downloader_df_index[date_from: date_to]
            spider_select_df = spider_df_index[date_from: date_to]

            date_from = calc_date_from.strftime('%Y-%m-%d')
            # 上記の期間抽出されたデータフレームより集計結果を求める。
            self.aggregate_result_set(robots_select_df, ['spider_name', 'robots_response_status'],
                                      date_from, self.robots_result_df)
            self.aggregate_result_set(downloader_select_df, ['spider_name', 'downloader_response_status'],
                                      date_from, self.downloader_result_df)
            self.aggregate_result_set(spider_select_df, ['spider_name'],
                                      date_from, self.spider_result_df)

            # 日付リストを作成
            date_list.append(date_from)

        # 日付別のスパイダー一覧を作成する。
        spider_list: pd.Series = (
            self.spider_df['spider_name'].drop_duplicates())
        #spider_by_date: list = list(itertools.product(date_list, spider_list))
        spider_by_date: list = [[date, spider]
                                for date, spider in itertools.product(date_list, spider_list)]
        # print('===spider一覧の確認\n',spider_list)
        # print('===\n',spider_by_date)
        spider_by_date_df = pd.DataFrame(spider_by_date, columns=[
                                         'aggregate_base_term', 'spider_name'])
        # print('===\n',spider_by_date_df.to_dict())

        #df2 = pd.merge(key_date_df, df2, how="left").sort_values(["key", "date"])

        # 各データフレームに対してソートを行う。
        df_sort_list: list[tuple[dict[str, pd.DataFrame], list]] = [
            (self.robots_result_df,
             ['spider_name', 'aggregate_base_term', 'robots_response_status']),
            (self.downloader_result_df,
             ['spider_name', 'aggregate_base_term', 'downloader_response_status']),
            (self.spider_result_df,
             ['spider_name', 'aggregate_base_term']),
        ]
        for type in ['sum', 'mean', 'min', 'max']:
            for dataframes, sort_key in df_sort_list:
                #dataframes[type].sort_values(by=sort_key, inplace=True)
                dataframes[type] = pd.merge(spider_by_date_df, dataframes[type],how='left').sort_values(
                    by=sort_key).fillna('')

        spider_result_all_df = pd.merge(self.spider_result_df['sum'],
                     self.spider_result_df['mean'],
                     suffixes=['', '_mean'],
                     on=['aggregate_base_term', 'spider_name'])
        spider_result_all_df = pd.merge(spider_result_all_df, self.spider_result_df['min'],
                     suffixes=['', '_min'],
                     on=['aggregate_base_term', 'spider_name'])
        spider_result_all_df = pd.merge(spider_result_all_df, self.spider_result_df['max'],
                     suffixes=['', '_max'],
                     on=['aggregate_base_term', 'spider_name'])

        return spider_result_all_df


        # self.robots_result_df['sum'].sort_values(
        #     by=['spider_name', 'aggregate_base_term', 'robots_response_status'], inplace=True)
        # self.downloader_result_df['sum'].sort_values(
        #     by=['spider_name', 'aggregate_base_term', 'downloader_response_status'], inplace=True)
        # self.spider_result_df['sum'].sort_values(
        #     by=['spider_name', 'aggregate_base_term'], inplace=True)

        # 他のソートもやらないと、、、

        #print('===\n', self.spider_result_df['sum'].to_dict())
        #print('===\n', self.spider_result_df['mean'].to_dict())
        #print('===\n', self.spider_result_df['min'].to_dict())
        #print('===\n', self.spider_result_df['max'].to_dict())

    def aggregate_result_set(self, select_df: pd.DataFrame, groupby: list, aggregate_base_term: str, result_df: dict[str, pd.DataFrame]):
        ''''''
        #{'sum': df, 'mean': df, 'min': df, 'max': df}
        _ = select_df.groupby(by=groupby, as_index=False).sum()
        _['aggregate_base_term'] = aggregate_base_term
        result_df['sum'] = pd.concat([result_df['sum'], _])
        # print(result_df['sum'].to_dict())

        _ = select_df.groupby(groupby, as_index=False).mean()
        _['aggregate_base_term'] = aggregate_base_term
        result_df['mean'] = pd.concat([result_df['mean'], _])

        _ = select_df.groupby(groupby, as_index=False).min()
        _['aggregate_base_term'] = aggregate_base_term
        result_df['min'] = pd.concat([result_df['min'], _])

        _ = select_df.groupby(groupby, as_index=False).max()
        _['aggregate_base_term'] = aggregate_base_term
        result_df['max'] = pd.concat([result_df['max'], _])

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
