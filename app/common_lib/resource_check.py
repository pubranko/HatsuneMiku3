import sys
import psutil
import logging

def resource_check() -> dict:
    '''
    CPU、メモリースワップメモリーの使用状況をチェックする。
    psutilで取得した値をログへ出力しdictで返す。
    cpu_percent, memory_used, memory_total, memory_available, memory_percent, swap_memory_used, swap_memory_total, swap_memory_percent
    '''

    memory = psutil.virtual_memory()
    swap_memory = psutil.swap_memory()
    resource: dict = {
        'cpu_percent': psutil.cpu_percent(),
        'memory_used': memory.used,
        'memory_total': memory.total,
        'memory_available': memory.available,
        'memory_percent': memory.percent,
        'swap_memory_used': swap_memory.used,
        'swap_memory_total': swap_memory.total,
        'swap_memory_percent': swap_memory.percent,
    }

    function_name:str = sys._getframe().f_code.co_name
    logger = logging.getLogger(function_name)

    logger.info('=== ＣＰＵ使用率 : ' + str(psutil.cpu_percent()) + '%')
    logger.info('=== メモリー使用状況')
    logger.info('=== used       / total      / available  / percent')
    logger.info(f'=== {str(memory.used)} / {str(memory.total)} / {str(memory.available)} / {str(memory.percent)}')
    logger.info('=== スワップメモリー使用状況')
    logger.info('=== used       / total      / percent')
    logger.info(f'=== {str(swap_memory.used)}   / {str(swap_memory.total)} / {str(swap_memory.percent)}')

    return resource
