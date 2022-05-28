from __future__ import annotations
import pandas as pd


class ScraperPatternReportData:
    '''
    スクレイパーパターン情報の使用状況のデータフレームを作成する。
    '''
    scraper_pattern_master_df: pd.DataFrame
    scraper_pattern_counter_df: pd.DataFrame
    result_df: pd.DataFrame

    def __init__(self):
        # データフレーム（マスター）
        self.scraper_pattern_master_df = pd.DataFrame({
            'domain': [],
            'scraper_item': [],
            'pattern': [],
            'priority': [],
            # 'count_of_use': [],
        })

        # データフレーム（カウント用）
        self.scraper_pattern_counter_df = pd.DataFrame({
            'domain': [],
            'scraper_item': [],
            'pattern': [],
            # 'priority': [],
            'count_of_use': [],
        })

    def scraper_info_master_store(self, record: dict) -> None:
        '''引数のレコードを基にpandasのデータフレーム（マスター）を作成する。'''
        _ = pd.DataFrame([record])
        self.scraper_pattern_master_df = pd.concat(
            [self.scraper_pattern_master_df, _], ignore_index=True)
        # self.scraper_pattern_master_df = self.scraper_pattern_master_df.append(
        #     record, ignore_index=True)

    def scraper_info_counter_store(self, record: dict) -> None:
        '''引数のレコードを基にpandasのデータフレーム（カウント用）を作成する。'''
        _ = pd.DataFrame([record])
        self.scraper_pattern_counter_df = pd.concat(
            [self.scraper_pattern_counter_df, _], ignore_index=True)
        # self.scraper_pattern_counter_df = self.scraper_pattern_counter_df.append(
        #     record, ignore_index=True)

    def scraper_info_analysis(self) -> None:
        '''
        マスターとカウント用のデータフレームで互いに足りないkeyを穴埋めしたデータフレーム（結果）を生成する。
        ※マスターにはcount_of_useがなく、カウント用にはpriorityがない。
        '''
        self.result_df = pd.merge(self.scraper_pattern_master_df,
                                  self.scraper_pattern_counter_df.groupby(
                                      by=['domain', 'scraper_item', 'pattern'], as_index=False).sum(),
                                  on=['domain', 'scraper_item', 'pattern'],
                                  how='outer').fillna(0)
