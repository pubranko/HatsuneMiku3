from datetime import datetime
from typing import Any, Final
import pandas as pd
from copy import deepcopy
import itertools


class StatsInfoCollectData:
    '''
    スパイダーごとに1日分のデータの集計を行う。
    ・入力データを1けんづつpandasのdataframeに格納する。
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
    # spiderの一覧
    spider_list: pd.Series

    #######################
    # 定数
    #######################
    RECORD_TYPE: Final[str] = 'record_type'
    '''定数: record_type'''
    START_TIME: Final[str] = 'start_time'
    '''定数: start_time'''
    SPIDER_NAME: Final[str] = 'spider_name'
    '''定数: spider_name'''
    PARAMETER: Final[str] = 'parameter'
    '''定数: parameter'''
    SPIDER_STATS: Final[str] = 'spider_stats'
    '''定数: spider_stats'''
    ROBOTS_RESPONSE_STATUS: Final[str] = 'robots_response_status'
    '''定数: robots_response_status'''
    DOWNLOADER_RESPONSE_STATUS: Final[str] = 'downloader_response_status'
    '''定数: downloader_response_status'''
    COUNT: Final[str] = 'count'
    '''定数: count'''
    TIME_PERIOD_HOUR: Final[str] = 'time_period_hour'
    '''定数: time_period_hour'''
    LOG_COUNT_CRITICAL: Final[str] = 'log_count/CRITICAL'
    '''定数: log_count/CRITICAL'''
    LOG_COUNT_ERROR: Final[str] = 'log_count/ERROR'
    '''定数: log_count/ERROR'''
    LOG_COUNT_WARNING: Final[str] = 'log_count/WARNING'
    '''定数: log_count/WARNING'''
    ELAPSED_TIME_SECONDS: Final[str] = 'elapsed_time_seconds'
    '''定数: elapsed_time_seconds'''
    MEMUSAGE_MAX: Final[str] = 'memusage/max'
    '''定数: memusage/max'''
    DOWNLOADER_REQUEST_COUNT: Final[str] = 'downloader/request_count'
    '''定数: downloader/request_count'''
    DOWNLOADER_RESPONSE_COUNT: Final[str] = 'downloader/response_count'
    '''定数: downloader/response_count'''
    REQUEST_DEPTH_MAX: Final[str] = 'request_depth_max'
    '''定数: request_depth_max'''
    DOWNLOADER_RESPONSE_BYTES: Final[str] = 'downloader/response_bytes'
    '''定数: downloader/response_bytes'''
    RETRY_COUNT: Final[str] = 'retry/count'
    '''定数: retry/count'''
    ITEM_SCRAPED_COUNT: Final[str] = 'item_scraped_count'
    '''定数: item_scraped_count'''
    FINISH_REASON: Final[str] = 'finish_reason'
    '''定数: finish_reason'''
    AGGREGATE_BASE_TERM: Final[str] = 'aggregate_base_term'
    '''定数: aggregate_base_term'''

    AGGREGATE_TYPE__SUM: Final[str] = 'sum'
    '''定数(value): 集計タイプ 合計'''
    AGGREGATE_TYPE__MEAN: Final[str] = 'mean'
    '''定数(value): 集計タイプ 平均'''
    AGGREGATE_TYPE__MIN: Final[str] = 'min'
    '''定数(value): 集計タイプ 最小'''
    AGGREGATE_TYPE__MAX: Final[str] = 'max'
    '''定数(value): 集計タイプ 最大'''

    def __init__(self):
        self.robots_df = pd.DataFrame({
            self.RECORD_TYPE: [],
            self.START_TIME: [],
            self.TIME_PERIOD_HOUR: [],
            self.SPIDER_NAME: [],
            self.ROBOTS_RESPONSE_STATUS: [],
            self.COUNT: [],
        })
        self.downloader_df = pd.DataFrame({
            self.RECORD_TYPE: [],
            self.START_TIME: [],
            self.TIME_PERIOD_HOUR: [],
            self.SPIDER_NAME: [],
            self.DOWNLOADER_RESPONSE_STATUS: [],
            self.COUNT: [],
        })
        self.spider_df = pd.DataFrame({
            self.RECORD_TYPE: [],
            self.START_TIME: [],
            self.TIME_PERIOD_HOUR: [],
            self.SPIDER_NAME: [],
            self.LOG_COUNT_CRITICAL: [],
            self.LOG_COUNT_ERROR: [],
            self.LOG_COUNT_WARNING: [],
            self.ELAPSED_TIME_SECONDS: [],
            self.MEMUSAGE_MAX: [],
            self.DOWNLOADER_REQUEST_COUNT: [],
            self.DOWNLOADER_RESPONSE_COUNT: [],
            self.REQUEST_DEPTH_MAX: [],
            self.DOWNLOADER_RESPONSE_BYTES: [],
            self.RETRY_COUNT: [],
            self.ITEM_SCRAPED_COUNT: [],
            self.FINISH_REASON: [],
        })

        robots_result_col = {
            self.AGGREGATE_BASE_TERM: [],
            self.SPIDER_NAME: [],
            self.ROBOTS_RESPONSE_STATUS: [],
            self.COUNT: [],
        }
        downloader_result_col = {
            self.AGGREGATE_BASE_TERM: [],
            self.SPIDER_NAME: [],
            self.DOWNLOADER_RESPONSE_STATUS: [],
            self.COUNT: [],
        }
        spider_result_col = {
            self.AGGREGATE_BASE_TERM: [],
            self.SPIDER_NAME: [],
            self.LOG_COUNT_CRITICAL: [],
            self.LOG_COUNT_ERROR: [],
            self.LOG_COUNT_WARNING: [],
            self.ELAPSED_TIME_SECONDS: [],
            self.MEMUSAGE_MAX: [],
            self.DOWNLOADER_REQUEST_COUNT: [],
            self.DOWNLOADER_RESPONSE_COUNT: [],
            self.REQUEST_DEPTH_MAX: [],
            self.DOWNLOADER_RESPONSE_BYTES: [],
            self.RETRY_COUNT: [],
            self.ITEM_SCRAPED_COUNT: [],
        }

        #{'sum': df, 'mean': df, 'min': df, 'max': df}
        self.robots_result_df: dict[str, pd.DataFrame] = {
            self.AGGREGATE_TYPE__SUM: pd.DataFrame(robots_result_col),
            self.AGGREGATE_TYPE__MEAN: pd.DataFrame(robots_result_col),
            self.AGGREGATE_TYPE__MIN: pd.DataFrame(robots_result_col),
            self.AGGREGATE_TYPE__MAX: pd.DataFrame(robots_result_col),
        }
        self.downloader_result_df: dict[str, pd.DataFrame] = {
            self.AGGREGATE_TYPE__SUM: pd.DataFrame(downloader_result_col),
            self.AGGREGATE_TYPE__MEAN: pd.DataFrame(downloader_result_col),
            self.AGGREGATE_TYPE__MIN: pd.DataFrame(downloader_result_col),
            self.AGGREGATE_TYPE__MAX: pd.DataFrame(downloader_result_col),
        }
        self.spider_result_df: dict[str, pd.DataFrame] = {
            self.AGGREGATE_TYPE__SUM: pd.DataFrame(spider_result_col),
            self.AGGREGATE_TYPE__MEAN: pd.DataFrame(spider_result_col),
            self.AGGREGATE_TYPE__MIN: pd.DataFrame(spider_result_col),
            self.AGGREGATE_TYPE__MAX: pd.DataFrame(spider_result_col),
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
                _ = pd.DataFrame([{
                    self.RECORD_TYPE: self.ROBOTS_RESPONSE_STATUS,
                    self.START_TIME: start_time,
                    self.TIME_PERIOD_HOUR: start_time.strftime('%H'),
                    self.SPIDER_NAME: spider_name,
                    self.ROBOTS_RESPONSE_STATUS: str(key).replace('robotstxt/response_status_count/', ''),
                    self.COUNT: value,
                }])
                #self.robots_df = self.robots_df.append(_, ignore_index=True)
                self.robots_df = pd.concat([self.robots_df, _], ignore_index=True)
            if 'downloader/response_status_count/' in key:
                _ = pd.DataFrame([{
                    self.RECORD_TYPE: self.DOWNLOADER_RESPONSE_STATUS,
                    self.START_TIME: start_time,
                    self.TIME_PERIOD_HOUR: start_time.strftime('%H'),
                    self.SPIDER_NAME: spider_name,
                    self.DOWNLOADER_RESPONSE_STATUS: str(key).replace('downloader/response_status_count/', ''),
                    self.COUNT: value,
                }])
                self.downloader_df = pd.concat([self.downloader_df, _], ignore_index=True)

        _ = pd.DataFrame([{
            self.RECORD_TYPE: self.SPIDER_STATS,
            self.START_TIME: start_time,
            self.TIME_PERIOD_HOUR: start_time.strftime('%H'),
            self.SPIDER_NAME: spider_name,
            self.LOG_COUNT_CRITICAL: stats['log_count_CRITICAL'] if 'log_count_CRITICAL' in stats else 0,
            self.LOG_COUNT_ERROR: stats['log_count_ERROR'] if 'log_count_ERROR' in stats else 0,
            self.LOG_COUNT_WARNING: stats['log_count_WARNING'] if 'log_count_WARNING' in stats else 0,
            self.ELAPSED_TIME_SECONDS: stats['elapsed_time_seconds'] if 'elapsed_time_seconds' in stats else 0,
            self.MEMUSAGE_MAX: stats['memusage_max'] if 'memusage_max' in stats else 0,
            self.DOWNLOADER_REQUEST_COUNT: stats['downloader_request_count'] if 'downloader_request_count' in stats else 0,
            self.DOWNLOADER_RESPONSE_COUNT: stats['downloader_response_count'] if 'downloader_response_count' in stats else 0,
            self.REQUEST_DEPTH_MAX: stats['request_depth_max'] if 'request_depth_max' in stats else 0,
            self.DOWNLOADER_RESPONSE_BYTES: stats['downloader_response_bytes'] if 'downloader_response_bytes' in stats else 0,
            self.RETRY_COUNT: stats['retry_count'] if 'retry_count' in stats else 0,
            self.ITEM_SCRAPED_COUNT: stats['item_scraped_count'] if 'item_scraped_count' in stats else 0,
            self.FINISH_REASON: stats['finish_reason'] if 'finish_reason' in stats else 0,
            # 'record_type': 'spider_stats',
            # 'start_time': start_time,
            # 'time_period_hour': start_time.strftime('%H'),
            # 'spider_name': spider_name,
            # 'log_count/CRITICAL': stats['log_count/CRITICAL'] if 'log_count/CRITICAL' in stats else 0,
            # 'log_count/ERROR': stats['log_count/ERROR'] if 'log_count/ERROR' in stats else 0,
            # 'log_count/WARNING': stats['log_count/WARNING'] if 'log_count/WARNING' in stats else 0,
            # 'elapsed_time_seconds': stats['elapsed_time_seconds'] if 'elapsed_time_seconds' in stats else 0,
            # 'memusage/max': stats['memusage/max'] if 'memusage/max' in stats else 0,
            # 'downloader/request_count': stats['downloader/request_count'] if 'downloader/request_count' in stats else 0,
            # 'downloader/response_count': stats['downloader/response_count'] if 'downloader/response_count' in stats else 0,
            # 'request_depth_max': stats['request_depth_max'] if 'request_depth_max' in stats else 0,
            # 'downloader/response_bytes': stats['downloader/response_bytes'] if 'downloader/response_bytes' in stats else 0,
            # 'retry/count': stats['retry/count'] if 'retry/count' in stats else 0,
            # 'item_scraped_count': stats['item_scraped_count'] if 'item_scraped_count' in stats else 0,
            # 'finish_reason': stats['finish_reason'] if 'finish_reason' in stats else 0,
        }])
        self.spider_df = pd.concat([self.spider_df, _], ignore_index=True)

    def dataframe_recovery(self, stats_info_collect_record: dict) -> None:
        '''引数のレコードを基にpandasのデータフレームを復元する。'''

        record_df = pd.DataFrame([stats_info_collect_record])

        if stats_info_collect_record[self.RECORD_TYPE] == self.ROBOTS_RESPONSE_STATUS:
            self.robots_df = pd.concat([self.robots_df,record_df],
                ignore_index=True)
        elif stats_info_collect_record[self.RECORD_TYPE] == self.DOWNLOADER_RESPONSE_STATUS:
            self.downloader_df = pd.concat([self.downloader_df,record_df],
                ignore_index=True)
        elif stats_info_collect_record[self.RECORD_TYPE] == self.SPIDER_STATS:
            self.spider_df = pd.concat([self.spider_df,record_df],
                ignore_index=True)
        # if stats_info_collect_record['record_type'] == self.ROBOTS_RESPONSE_STATUS:
        #     self.robots_df = self.robots_df.append(
        #         stats_info_collect_record, ignore_index=True)
        # elif stats_info_collect_record['record_type'] == self.DOWNLOADER_RESPONSE_STATUS:
        #     self.downloader_df = self.downloader_df.append(
        #         stats_info_collect_record, ignore_index=True)
        # elif stats_info_collect_record['record_type'] == 'spider_stats':
        #     self.spider_df = self.spider_df.append(
        #         stats_info_collect_record, ignore_index=True)

    def date_time_set_index(self, columns: str, df: pd.DataFrame):
        '''
        引数で指定されたデータフレームに対し、引数で指定したカラムの
        インデックスを追加したデータフレームを返す。
        '''
        df[columns] = pd.to_datetime(df[columns])
        df.set_index([self.START_TIME]).tz_localize(
            'UTC').tz_convert('Asia/Tokyo')
        return df.set_index([self.START_TIME]).tz_localize('UTC').tz_convert('Asia/Tokyo')

    def stats_analysis_exec(self, datetime_term_list: list[tuple[datetime, datetime]]) -> pd.DataFrame:
        '''引数で渡された集計期間リストごとに解析を実行'''
        # start_timeをインデックスとしたdataframを生成
        # index生成後ソートを実行する。※ソートしないと将来的にエラーになると警告を受ける。
        robots_df_index = self.date_time_set_index(
            self.START_TIME, self.robots_df).sort_index()
        downloader_df_index = self.date_time_set_index(
            self.START_TIME, self.downloader_df).sort_index()
        spider_df_index = self.date_time_set_index(
            self.START_TIME, self.spider_df).sort_index()

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
            self.aggregate_result_set(robots_select_df, [self.SPIDER_NAME, self.ROBOTS_RESPONSE_STATUS],
                                      date_from, self.robots_result_df)
            self.aggregate_result_set(downloader_select_df, [self.SPIDER_NAME, self.DOWNLOADER_RESPONSE_STATUS],
                                      date_from, self.downloader_result_df)
            self.aggregate_result_set(spider_select_df, [self.SPIDER_NAME],
                                      date_from, self.spider_result_df)

            # 日付リストを作成
            date_list.append(date_from)

        # 日付別のスパイダー一覧を作成する。
        self.spider_list: pd.Series = (
            self.spider_df[self.SPIDER_NAME].drop_duplicates().sort_values()
            )
        spider_by_date: list = [[date, spider]
                                for date, spider in itertools.product(date_list, self.spider_list)]
        spider_by_date_df = pd.DataFrame(spider_by_date, columns=[
                                         self.AGGREGATE_BASE_TERM, self.SPIDER_NAME])

        # 各データフレームに対してソートを行う。
        df_sort_list: list[tuple[dict[str, pd.DataFrame], list]] = [
            (self.robots_result_df,
             [self.SPIDER_NAME, self.AGGREGATE_BASE_TERM, self.ROBOTS_RESPONSE_STATUS]),
            (self.downloader_result_df,
             [self.SPIDER_NAME, self.AGGREGATE_BASE_TERM, self.DOWNLOADER_RESPONSE_STATUS]),
            (self.spider_result_df,
             [self.SPIDER_NAME, self.AGGREGATE_BASE_TERM]),
        ]
        for type in [self.AGGREGATE_TYPE__SUM, self.AGGREGATE_TYPE__MEAN,
                     self.AGGREGATE_TYPE__MIN, self.AGGREGATE_TYPE__MAX]:
            for dataframes, sort_key in df_sort_list:
                dataframes[type] = pd.merge(spider_by_date_df, dataframes[type],how='left').sort_values(
                    by=sort_key).fillna('')

        spider_result_all_df = pd.merge(self.spider_result_df[self.AGGREGATE_TYPE__SUM],
                     self.spider_result_df[self.AGGREGATE_TYPE__MEAN],
                     suffixes=['', '_mean'],
                     on=[self.AGGREGATE_BASE_TERM, self.SPIDER_NAME])
        spider_result_all_df = pd.merge(spider_result_all_df, self.spider_result_df[self.AGGREGATE_TYPE__MIN],
                     suffixes=['', '_min'],
                     on=[self.AGGREGATE_BASE_TERM, self.SPIDER_NAME])
        spider_result_all_df = pd.merge(spider_result_all_df, self.spider_result_df[self.AGGREGATE_TYPE__MAX],
                     suffixes=['', '_max'],
                     on=[self.AGGREGATE_BASE_TERM, self.SPIDER_NAME])

        return spider_result_all_df

    def aggregate_result_set(self, select_df: pd.DataFrame, groupby: list, aggregate_base_term: str, result_df: dict[str, pd.DataFrame]):
        ''''''
        #{'sum': df, 'mean': df, 'min': df, 'max': df}
        _ = select_df.groupby(by=groupby, as_index=False).sum()
        _[self.AGGREGATE_BASE_TERM] = aggregate_base_term
        result_df[self.AGGREGATE_TYPE__SUM] = pd.concat([result_df[self.AGGREGATE_TYPE__SUM], _]).round(2)

        _ = select_df.groupby(groupby, as_index=False).mean()
        _[self.AGGREGATE_BASE_TERM] = aggregate_base_term
        result_df[self.AGGREGATE_TYPE__MEAN] = pd.concat([result_df[self.AGGREGATE_TYPE__MEAN], _]).round(2)

        _ = select_df.groupby(groupby, as_index=False).min()
        _[self.AGGREGATE_BASE_TERM] = aggregate_base_term
        result_df[self.AGGREGATE_TYPE__MIN] = pd.concat([result_df[self.AGGREGATE_TYPE__MIN], _]).round(2)

        _ = select_df.groupby(groupby, as_index=False).max()
        _[self.AGGREGATE_BASE_TERM] = aggregate_base_term
        result_df[self.AGGREGATE_TYPE__MAX] = pd.concat([result_df[self.AGGREGATE_TYPE__MAX], _]).round(2)


    stats_image = {
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
    }
