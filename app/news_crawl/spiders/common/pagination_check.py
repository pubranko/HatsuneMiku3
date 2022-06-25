import re
from urllib.parse import urlparse, parse_qs
from urllib.parse import ParseResult
from logging import Logger,LoggerAdapter


class PaginationCheck():

    # ページネーションで追加済みのurlリスト
    pagination_selected_urls: set[str] = set()

    def check(self, link_url: str, crawl_target_urls: list, logger: LoggerAdapter, spider_name: str) -> bool:
        '''
        チェックしたいurl(link_url)に対して、既にクロール対象となったurl(crawl_target_urls)の別ページかチェックを行う。
        '''
        check_flg: bool = False  # ページネーションのリンクの場合、Trueとする。
        # チェック対象のurlを解析
        link_parse: ParseResult = urlparse(link_url)
        # 解析したクエリーをdictへ変換 page=2&a=1&b=2 -> {'page': ['2'], 'a': ['1'], 'b': ['2']}
        link_query: dict = parse_qs(link_parse.query)

        # 追加リクエスト済み情報の準備
        pagination_selected_pathes: set = set()
        pagination_selected_same_path_queries: list = []
        for pagination_selected_url in self.pagination_selected_urls:
            _ = urlparse(pagination_selected_url)
            pagination_selected_pathes.add(_.path)
            if link_parse.path == _.path:
                pagination_selected_same_path_queries.append(parse_qs(_.query))

        # sitemapから取得したurlより順にチェック
        for _ in crawl_target_urls:
            # sitemapから取得したurlを解析
            crawl_target_parse: ParseResult = urlparse(_)
            # netloc（hostnameだけでなくportも含む）が一致すること
            if crawl_target_parse.netloc == link_parse.netloc:
                # まだ同一ページの追加リクエストされていない場合（path部分で判定）
                if not link_parse.path in pagination_selected_pathes:
                    # パスの末尾にページが付与されているケースの場合、追加リクエストの対象とする。
                    # 例）https://www.sankei.com/article/20210321-VW5B7JJG7JKCBG5J6REEW6ZTBM/
                    #     https://www.sankei.com/article/20210321-VW5B7JJG7JKCBG5J6REEW6ZTBM/2/
                    _ = re.compile(r'/[0-9]{1,3}/*$')
                    if re.search(_, link_parse.path):
                        # pathの末尾のページ情報を削除
                        # 例）〜OYT1T50226/2/ -> 〜OYT1T50226
                        link_type1 = _.sub('', link_parse.path)
                        # 末尾のスラッシュがあれば削除
                        _ = re.compile(r'/$')
                        crawl_type1 = _.sub('', crawl_target_parse.path)
                        # ページ情報部を除いて比較し一致した場合
                        if crawl_type1 == link_type1:
                            logger.info(
                                f'=== {spider_name} ページネーション(type1) : {link_url}')
                            check_flg = True

                    # 拡張子除去後の末尾にページが付与されているケースの場合、追加リクエストの対象とする。
                    # 例）https://www.sankei.com/politics/news/210521/plt2105210030-n1.html
                    #     https://www.sankei.com/politics/news/210521/plt2105210030-n2.html
                    _ = re.compile(r'[^0-9][0-9]{1,3}\.(html|htm)$')
                    if re.search(_, link_parse.path):
                        # 例）〜n1.html -> 〜n
                        link_type2 = _.sub('', link_parse.path)
                        crawl_type2 = _.sub('', crawl_target_parse.path)
                        # 末尾の拡張子やページ情報を除いて比較し一致した場合
                        if crawl_type2 == link_type2:
                            logger.info(
                                f'=== {spider_name} ページネーション(type2) : {link_url}')
                            check_flg = True

                # クエリーにページが付与されているケースの場合、追加リクエストの対象とする。
                # ただし、以下の場合は対象外。
                # ・既に同一ページの追加リクエスト済みの場合。
                # ・１ページ目の場合。※sitemap側でリクエスト済みのため。
                # 例）https://webronza.asahi.com/national/articles/2022042000004.html
                #     https://webronza.asahi.com/national/articles/2022042000004.html?a=b&c=d
                #     https://webronza.asahi.com/national/articles/2022042000004.html?page=1&a=b&e=f
                #     https://webronza.asahi.com/national/articles/2022042000004.html?page=1&m=n&g=h
                #     https://webronza.asahi.com/national/articles/2022042000004.html?page=2&a=b&e=f
                #     https://webronza.asahi.com/national/articles/2022042000004.html?page=2&m=n&g=h
                if crawl_target_parse.path == link_parse.path:
                    # リンクのクエリーにページ指定と思われるkeyの存在チェック （複数該当することは無いことを祈る、、、）
                    page_keys = ['page', 'pagination', 'pager', 'p']
                    link_query_selected_items: list[tuple] = []
                    for link_query_key, link_query_value in link_query.items():
                        if link_query_key in page_keys:
                            link_query_selected_items.append(
                                (link_query_key, link_query_value))

                    # linkにpege系クエリーがあった場合、
                    for link_query_selected_item in link_query_selected_items:
                        check_flg = True
                        for same_path_query in pagination_selected_same_path_queries:
                            # keyが一致
                            if link_query_selected_item[0] in same_path_query:
                                # valueが一致(同一ページ)した場合は対象外
                                if link_query_selected_item[1][0] == same_path_query[link_query_selected_item[0]][0]:
                                    check_flg = False
                                # page=1は対象外
                                elif link_query_selected_item[1][0] == str(1):
                                    check_flg = False
                        if check_flg:
                            logger.info(
                                f'=== {spider_name} ページネーション(type3) : {link_url}')

        # クロール対象となったurlを保存
        if check_flg:
            self.pagination_selected_urls.add(link_url)

        return check_flg
