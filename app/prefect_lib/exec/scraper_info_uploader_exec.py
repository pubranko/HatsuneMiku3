from prefect_lib.flow.scraper_info_uploader_flow import flow
flow.run(parameters=dict(
    scraper_info_by_domain_files=[
        "nikkei_com.json",
        #"mainichi_jp.json",
        #"epochtimes_jp.json",
        #'yomiuri_co_jp.json',
        #'yomiuri_co_jp_e1.json',
        #'yomiuri_co_jp_e2.json',
        #'yomiuri_co_jp_e3.json',
        #'yomiuri_co_jp_e4.json',
        #'yomiuri_co_jp_e5.json',
        #'yomiuri_co_jp_e6.json',
        #'yomiuri_co_jp_e7.json',
        #'yomiuri_co_jp.json',
    ],
))
