import logging
import tempfile


# 各Flowのロギング設定を行う。
if not'log_file_path' in globals():
    # prefect_lib/flow/crawl_urls_sync_check_flow.py => prefect_lib/logs/crawl_urls_sync_check_flow.log
    # log_file_path = os.path.join(
    #     'logs', os.path.splitext(os.path.basename(__file__))[0] + '.log')
    log_file_path = tempfile.NamedTemporaryFile(dir='./logs').name
    logging.basicConfig(level=logging.DEBUG, filemode="w+", filename=log_file_path,
                        format='%(asctime)s %(levelname)s [%(name)s] : %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    #print('tempfile ', log_file_path)
