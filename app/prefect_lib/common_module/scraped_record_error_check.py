import logging
from logging import Logger

logger: Logger = logging.getLogger('prefect.common_module.scraped_record_error_check')
warning_flg:bool = False

def scraped_record_error_check(record:dict) -> bool:
    '''
    渡されたrecordの中に、該当するkeyがあるかチェックする。
    '''

    def check(record:dict,item:str,log_item:str) -> None:
        global warning_flg
        if item not in record:
            warning_flg = True
            logger.warning(
                '=== ワーニング：' + item + 'なし : ' + str(record[log_item]))
        elif len(str(record[item])) == 0 or record[item] == None:
            warning_flg = True
            logger.warning(
                '=== ワーニング：' + item + ' = データなし :' + str(record[log_item]))

    global warning_flg
    warning_flg = False
    # データチェック
    check(record,'url','_id')
    check(record,'title','url')
    check(record,'article','url')
    check(record,'publish_date','url')

    return warning_flg