import re


def url_pattern_skip_check(url: str, kwargs: dict):
    '''
    spiderへの引数にurl_patternがある場合、urlパターンチェックを行う。
    チェックの結果パターンと不一致の場合、スキップ対象(True)を返す。
    チェックの結果パターンと一致した場合、スキップ対象外(False)を返す。
    '''
    crwal_flg:bool = False
    if 'url_pattern' in kwargs:   # url絞り込み指定あり
        pattern = re.compile(kwargs['url_pattern'])
        if pattern.search(url) == None:
            crwal_flg = True

    return crwal_flg