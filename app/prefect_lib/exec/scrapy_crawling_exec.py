from prefect_lib.flow.scrapy_crawling_flow import flow

flow.run(parameters=dict(
    spider_names=[
        "sankei_com_sitemap",
        "asahi_com_sitemap",
        "kyodo_co_jp_sitemap",
        "yomiuri_co_jp_sitemap",
        "jp_reuters_com_crawl",
        "epochtimes_jp_crawl",
        "mainichi_jp_crawl",
        "nikkei_com_crawl"
    ],
    spider_kwargs={
        'debug': 'Yes',
        'pages': '1,1',
        'lastmod_period_minutes': '120,',
        #'lastmod_period_minutes': '3840,3780',
        #'continued':'Yes',
        # 'direct_crawl_urls':[],
        #'crawl_point_non_update':'Yes',
        #'url_pattern':'https://www.yomiuri.co.jp/national/20220430-OYT1T50050',
        #'url_pattern':'https://mainichi.jp/articles/20220607/k00/00m/050/362000c',
        #'url_pattern':'https://jp.reuters.com/article/euronext-tech-idJPKBN2NO0TB',
        #'url_pattern':'https://www.epochtimes.jp/2022/06/107648.html',
    },
    following_processing_execution='Yes'    # 後続処理実行(scrapying,news_clip_masterへの登録,solrへの登録)
))
