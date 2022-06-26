from prefect_lib.flow.monthly_delete_flow import flow
flow.run(parameters=dict(
    collections_name=[
        'crawler_response',
        'crawler_logs',
        'asynchronous_report',
        'scraped_from_response',
        'news_clip_master',
        'controller',
    ],
    delete_period_from='2022-02',  # 月次削除を行うデータの基準年月
    delete_period_to='2022-05',  # 月次削除を行うデータの基準年月
))
