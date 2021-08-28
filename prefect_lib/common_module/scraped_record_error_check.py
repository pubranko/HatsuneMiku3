import logging
from logging import Logger

logger: Logger = logging.getLogger('prefect.common_module.scraped_record_error_check')

def scraped_record_error_check(record:dict) -> bool:
    '''
    渡されたrecordの中に、該当するkeyがあるかチェックする。
    '''

    # データチェック
    error_flg:bool = False
    if 'url' not in record:
        error_flg = True
        logger.error(
            '=== エラー：urlがありません。' + str(record['_id']))
    if 'title' not in record:
        error_flg = True
        logger.error(
            '=== エラー：titleがありません。 : ' + str(record['url']))
    if 'article' not in record:
        error_flg = True
        logger.error(
            '=== エラー：articleがありません。 : ' + str(record['url']))
    if 'publish_date' not in record:
        error_flg = True
        logger.error(
            '=== エラー：publish_dateがありません。 : ' + str(record['url']))

    return error_flg