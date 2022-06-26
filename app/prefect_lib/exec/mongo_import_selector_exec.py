from prefect_lib.flow.mongo_import_selector_flow import flow
flow.run(parameters=dict(
    collections_name=[
        'crawler_response',
        'scraped_from_response',
        'news_clip_master',
        'crawler_logs',
        'asynchronous_report',
        'controller',
    ],
    prefix='test',
    backup_dir_from='2022-02',    # prefect_lib.settings.BACKUP_BASE_DIR内のディレクトリを指定
    backup_dir_to='2022-05',    # prefect_lib.settings.BACKUP_BASE_DIR内のディレクトリを指定
))
