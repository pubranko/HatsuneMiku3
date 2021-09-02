2021/9/2残タスク確認
・mongo controllerの日付を文字列からISODate化 → 完了
・親サイトマップから子サイトマップのクローリングの実装

2021/9/2
・CrawlingContinuedSkipCheckによるクローリングの続き機能への改修完了。
・url_pattern_skip_checkによるurlのパターン抽出機能への改修完了。
2021/9/1
・LastmodPeriodMinutesSkipCheckによりlastmodの範囲指定機能実装完了
2021/8/31
scrapyの見直し
・selenium版sitemap_spiderの実装完成
・抽出条件の改善
・親サイトマップから子サイトマップのクローリングの実装

2021/8/30
scrapyの見直し
・selenium版sitemap_spiderの実装途中
prefect
・スクレイピングエラーの解消
  タイプ別のスクレイピング機能の実装
・url指定のスクレイピング機能実装

2021/8/29
コントロールの見直し
・crawler_controller → controllerへリネーム
・Prefect側でクロール可否を制御するコントロールを実装する。
  1. ドメイン単位で制御
  2. crawling、scrapyingごとに止める箇所を制御できるようにする。
  3. 配列でドメインをもたせる。配列に含まれるドメインの場合、停止対象とみなす。

mongoDB 
{
    document_type : stop_controller,
    crawling_stop_domain_list:[],
    scrayping_stop_domain_list:[],
}

2021/7/24

＜spiderでやるべきこと＞
１．__init__()
    １）crawler_controllerから以下の情報を取得
        ①クロール中ドメイン：document_type = crawling_domain
    ２）同一ドメインでクロール中のものがあるかチェック
        ①あれば(crawling_status=True)クロールを中止。
            a)ワーニングを発報
            b)クローリングログ(crawling_log)へ書き込み
            c)正常終了させる(sys.exit())
        ②なければ
            a)クロール中ドメイン：document_type = crawling_domainに、以下の情報を設定
                ・クローリングステータス(crawling_status):True(クロール中)
                ・開始時間(start_time):クローラーの開始時間
                ・終了時間(end_time):None
            b)sleep(1)
            c)再度クロール中ドメイン：document_type = crawling_domainをチェックし、
                ・開始時間(start_time)が同じであれば他と多重になっていないとみなし、後続の処理を実行。
                ・上記以外はクロール中止。
            d)crawler_controllerから以下の情報を取得
                前回情報：document_type = next_crawl_point

２．closed()で、crawler_controllerへ以下の情報を書き込み
    １）次回クロールポイント情報を書き込み
        ドキュメントタイプ(document_type):次回クロールポイント(next_crawl_point)
        ドメイン(domain):ドメイン名
        クローラー名(crawler_name):{
            サイトマップurl:{
                lastmod:
                url:
                crawling_start_time:
            },
            一覧ページurl:{
                url:
                lastmod:
                ,,,,,,,,,
            },
        }

    ２）クローリングログ(crawling_log)
        key:実行対象(Executive_target):news_crawl   #news_crawlやtwitterなどの区別用
        key:クローラー名(crawler_name):xxxxxx,
        key:クロール先ドメイン名(crawl_domain):xxxxxx,
        key:prefect関数名(prefect_lib_name):スパイダー名_type3、、、起動元の関数名取得できる？？？
        key:スパイダー名(spider_name):スパイダー名
        ログレコード(log_record):[
            開始時間:
            終了時間:
            エラーの有無:
            spiderのログオブジェクト:
        ]

３．拡張スパイダークラスは別フォルダへ移動

＜prefect_lib(patrol)でやるべきこと＞
１．クローラー特性クラス(CrawlerCharacteristic)を呼び出して、patrol対象のスパイダー情報等を取得
２．スパイダーの存在チェック(news_crawl/spider内に存在するかチェック)
    ・なければWraningを発報
    ※参考ph
        https://qiita.com/m_mizutani/items/
        https://base64.work/so/python/1421026
３．各スパイダーをマルチ処理でそれぞれ起動。
    ※参考ph
        https://tommysblog.net/python/python-subprocess-asynchronous/
        https://qiita.com/melka-blue/items/03cc2d7c68b7cfdbd110
        基本的に、python同士なので、multiprocessing でやるのが普通のようだ。

＜prefect_lib(at_any_time)でやるべきこと＞
上記patrolと同じ。参照先のクラスがat_any_timeに変わるのみ。


================================
クローラーごとの特性の管理クラス(CrawlerCharacteristic)
    def patrol         この中にあるlist[dict]を巡回対象とみなす。
        file:スパイダー名_type1.py
    def at_any_time      この中にあるlist[dict]は随時用とみなす。随時依頼があったときだけ実行する。
        file:スパイダー名_type2.py
    def save           保管庫。上記のlist[dict]はここに原本として保管される。使っていないソースもここに保管する。使うときだけ上記の関数にコピーする。

    管理項目
        実行対象(Executive_target):news_crawl   #news_crawlやtwitterなどの区別用
        クローラー名(crawler_name):xxxxxx,
        クロール先ドメイン名(crawl_domain):xxxxxx,
        discription:用途を記載する,
        関数名:スパイダー名_type3
        クールダウンタイム:nn,  前回終了後、最低限間を開ける時間（分単位）

クローラーコントローラー(crawler_controller)    ※直近の情報のみ管理
    ドキュメントタイプ(document_type):次回クロールポイント(next_crawl_point)
    ドメイン(domain):ドメイン名
    クローラー名(crawler_name):{
        サイトマップurl:{
            lastmod:
            url:
            crawling_start_time:
        },
        一覧ページurl:{
            url:
            lastmod:
            ,,,,,,,,,
        },
    }

    ※同一ドメインへ多重クロールを防止するためのステータス、開始・終了時間を管理。
    ドキュメントタイプ(document_type):クロール中ドメイン(crawling_domain)
    ドメイン(domain):ドメイン名
    プロセスid(process_id):
    プロセス名(process_name):
    開始時間(start_time):直近のクローラーの開始時間
    #クローリングステータス(crawling_status):True(クロール中)、False(待機中)
    #終了時間(end_time):クロール中はNone、アイドリング中は直近のクローラーの終了時間

クロールの実績管理DB(？)    ※ログとして管理
    (crawling_log):クローラーごとの実績管理
        key:実行対象(Executive_target):news_crawl   #news_crawlやtwitterなどの区別用
        key:クローラー名(crawler_name):xxxxxx,
        key:クロール先ドメイン名(crawl_domain):xxxxxx,
        key:prefect関数名(prefect_lib_name):スパイダー名_type3、、、起動元の関数名取得できる？？？
        key:スパイダー名(spider_name):スパイダー名
        ログレコード(log_record):[
            開始時間:
            終了時間:
            エラーの有無:
            spiderのログオブジェクト:
        ]

2021/7/23
    '''
    クローラーの特性はpythonのクラス定義でいいんじゃね？
    spiderから必要情報抽出する関数を作ってみるか。
    ステータスなど随時変わるものだけDBにするべきだ。
    引数のタイプをどうする？

    {#クローラーごとの特性を管理したい。巡回(patrol)の対象？非対象？
        レコードタイプ(record_type):crawer_info,
        クローラー名(crawler_name):xxxxxx,
        クロール先ドメイン名(crawl_domain):xxxxxx,
        discription:用途を記載する,

    ng    type:n,     レコードタイプ、クローラー名、typeでユニークとする。

        巡回タイプ(patrol_repeat_type):臨時(temporary)、日中(daily)、深夜(midnight)
        error_flg:True/False,   障害発生中の場合、ture。正常であればfalse
        クールダウンタイム:nn,  前回終了後、最低限間を開ける時間（分単位）
    }

    {#各ドメイン・クローラーごとに、前回実績のurl/lastmodを保存し、続きがわかるようにしたい。
        レコードタイプ(record_type):next_crawl_point
        ドメイン(domain):
        クローラー名(crawler_name):{
            検索先url:{ #中は色々
                latest_lastmod:
                latest_url:
                crawler_start_time:
                [検索先の最初の10件のurl,,,],
            },,,,
        },,,,
    }

    {#各ドメインごとに、現在クロール中か否かわかるようにしたい。（同一ドメインへの多重起動禁止機能）
        レコードタイプ(record_type):crawl_status_by_domain,
        ドメイン(domain):xxxxx,      #主キー
        ステータス(status):,    #crawling,stand-by,
        最後のクローラー(last_crawler_name):,
        最後のクロール時間(last_crawl_time):,
        最後のクロール結果(last_crawl_status):,     #正常(Normal)、エラー(Error)、致命的エラー(Fatal)

    }

    '''

2021/5/4　今後の進め方
１．クロールサイトの追加
　　１）朝日
　　２）読売
　　３）毎日
　　４）ロイター
　　５）日経
　　６）大紀元
２．スクレイピング基本処理（各サイトから基本項目を抽出し、mongoDBへ保存）
　　１）url、タイトル、本文、発行者、response_time、publish_datetime(またはpublish_date)、スクレイピングバージョン

{'mongo_id': ['〜'], 'url': '〜', 'title': '〜', 'article': '〜', 'issuer': ['〜'], 'update_count': 0, 
'id': '〜', '_version_': 1638412764864053248, 'response_time': '2019-01-20T19:24:59.014Z', 'publish_date': '2018-04-01T00:00:00Z'}

