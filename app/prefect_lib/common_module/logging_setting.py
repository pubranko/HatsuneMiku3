import logging
import tempfile
from shared.settings import DATA_DIR__LOGS


# 各Flowのロギング設定を行う。
if not'LOG_FILE_PATH' in globals():
    # prefect_lib/flow/crawl_urls_sync_check_flow.py => prefect_lib/logs/crawl_urls_sync_check_flow.log
    # LOG_FILE_PATH = os.path.join(
    #     'logs', os.path.splitext(os.path.basename(__file__))[0] + '.log')
    LOG_FILE_PATH = tempfile.NamedTemporaryFile(dir=DATA_DIR__LOGS).name
    logging.basicConfig(level=logging.DEBUG, filemode="w+", filename=LOG_FILE_PATH,
                        format='%(asctime)s %(levelname)s [%(name)s] : %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    #print('tempfile ', LOG_FILE_PATH)
