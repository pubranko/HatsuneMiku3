from datetime import datetime, timedelta


def term_days_Calculation(crawling_start_time: datetime, term_days: int, date_pattern: str) -> list:
    '''
    クロール開始時刻(crawling_start_time)を起点に、期間(term_days)分の日付リスト(文字列)を返す。
    日付の形式(date_pattern)には、datetime.strftimeのパターンを指定する。
    '''
    return [(crawling_start_time - timedelta(days=i)).strftime(date_pattern) for i in range(term_days)]
