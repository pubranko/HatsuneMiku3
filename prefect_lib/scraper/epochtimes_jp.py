import os
import sys
import logging
from logging import Logger

logger:Logger = logging.getLogger('prefect.' +
                        sys._getframe().f_code.co_name)

def exec(record:dict) -> dict:
    global logger
    logger.info('=== 中身の確認 : ' + str(record['url']))

    return {}