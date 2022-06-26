from prefect_lib.flow.mongo_export_selector_flow import flow
flow.run(parameters=dict(
    # collections=['asynchronous_report','controller','crawler_logs','crawler_response','news_clip_master','scraped_from_response'],
    collections_name=[
        'crawler_response',
        'scraped_from_response',  # 通常運用では不要なバックアップとなるがテスト用に実装している。
        'news_clip_master',
        'crawler_logs',
        'asynchronous_report',
        'controller',
    ],
    prefix='test2',   # export先のフォルダyyyy-mmの先頭に拡張した名前を付与する。
    export_period_from='2022-06',  # 月次エクスポートを行うデータの基準年月
    export_period_to='2022-06',  # 月次エクスポートを行うデータの基準年月
    crawler_response__registered=True,   # crawler_responseの場合、登録済みになったレコードのみエクスポートする場合True、登録済み以外のレコードも含めてエクスポートする場合False
))
