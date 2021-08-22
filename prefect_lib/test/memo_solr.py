import pysolr
from datetime import datetime

import requests

#c ='/home/mikuras/004_atelier/002_Brownie/solr-keystore.pem'
#c ='/home/mikuras/004_atelier/002_Brownie/solr-keystore.pem'
solr = pysolr.Solr(
    'https://localhost:8983/solr/mycore/',
    timeout=10,
    #verify=c,
    auth=('HatsuneMiku', 'sinfonii2017'),
    always_commit=True
)

print('=== 1 全件検索 ===')
results = solr.search('*:*')

print("    Saw {0} result(s).".format(len(results)))  # 結果の有無の調べ方

for result in results:
    print("    title '{0}'.".format(result['title']))

# 個別のフィールドを検索。or,and,で結合することもできる。また、not で否定することも可能。
print('=== 2 特定のフィールドで検索 ===')
results = solr.search(['article:8984 or article:8983'])
for result in results:  # 複数の検索結果を1つづつ処理
    print("    title '{0}'.".format(result['title']))

print('=== 3  === 検索結果のjsonを全て表示')
results = solr.search('article:8984')  # 個別のフィールドを検索
for result in results:  # 複数の検索結果を1つづつ処理
    print("    all '{0}'.".format(result))

print('=== 4  === ソートのやり方、取得するフィールドの絞り込みのやり方')
results = solr.search('article:8984 or article:8983', **{
    'sort': 'url asc,title desc,',  # ソートのやり方。 desc降順 asc昇順。 %20は空白に置き換えること。
    'fl': 'title,url',  # 取得したいフィールドを限定する場合、
})

for result in results:
    print("    all '{0}'.".format(result))

print('=== 5  === ハイライトのやり方、取得する開始件数／上限件数の指定のやり方')
results = solr.search('article:8984 or article:8983', **{
    'sort': 'url asc,title desc,',  # ソートのやり方。 desc降順 asc昇順。 %20は空白に置き換えること。
    'hl.fl': 'article',  # ハイライトを行うフィールド
    'hl': 'on',  # ハイライトをON
    'hl.simple.post': '</em>',  # ハイライトしたい文字の後ろに太字タグを設定
    'hl.simple.pre': '<em>',  # ハイライトしたい文字の前に太字タグを設定
    'start': 0,  # 検索結果の開始件数。※１件目より表示。
    'rows': 2,  # 検索結果の表示件数。※１件のみ表示。
})

for result in results:
    print("    all '{0}'.".format(result))

print(results.highlighting)
for result in results:
    print("    HightLight '{0}'.".format(result))

print('=== 6  === 絞り込み検索のやり方')
results = solr.search('title:テスト', **{
    'sort': 'url asc,title desc,',  # ソートのやり方。 desc降順 asc昇順。 %20は空白に置き換えること。
    'fq': ['title:3or2', 'article:8984'],  # 絞り込み検索で、複数の値で絞り込むときはfqにリストで渡す。
    # リストごとにand結合となる。リスト内の要素にorを入れることもできる。
})

for result in results:
    print("    all '{0}'.".format(result))

print('=== 7  === ファセットの取得のやり方')
results = solr.search('title:テスト', **{
    'facet': 'on',  # title＝テストで検索し、その結果をファセットとして取得する。
    'facet.field': ['title', 'article'],
})

print("    Saw {0} result(s).".format(
    len(results.facets['facet_fields']['article'])))
# ファセットより、実際に取得された値が格納されているリストを指定。
f = results.facets['facet_fields']['article']
l = [f[i:i + 2] for i in range(0, len(f), 2)]  # 上記で取り出したリストを2こづつのリストに変換
for result in l:  # 上記のリストを順に取り出す。
    print("    facets article '{0}'.".format(result))
'''
results内のfacetsには以下の5つの要素（辞書型）がある。
    facet_queries, facet_fields, facet_ranges, facet_intervals, facet_heatmaps
このうち、ファセット本体があるのは、facet_fieldsになるが、辞書型で格納されている。
辞書型のkey値は上記の例では'article'となり、辞書型の値はリスト型で格納されている。
そのリスト型は、[項目値1,項目値1の件数,項目値2,項目値2の件数,〜」のように格納されている。
それを効率良く取り出す方法を検討した結果上記の例となった。
'''
# 以下、同時に取得したtitle側のファセット
print("    Saw {0} result(s).".format(
    len(results.facets['facet_fields']['title'])))
f = results.facets['facet_fields']['title']  # ファセットより、実際に取得された値が格納されているリストを指定。
l = [f[i:i + 2] for i in range(0, len(f), 2)]  # 上記で取り出したリストを2こづつのリストに変換
for result in l:  # 上記のリストを順に取り出す。
    print("    facets title '{0}'.".format(result))
