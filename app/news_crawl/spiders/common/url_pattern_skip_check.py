import re
from typing import Optional


def url_pattern_skip_check(url: str, url_pattern: Optional[str]):
    '''
    spiderへの引数にurl_patternがある場合、urlパターンチェックを行う。
    チェックの結果パターンと不一致の場合、スキップ対象(True)を返す。
    チェックの結果パターンと一致した場合、スキップ対象外(False)を返す。
    '''
    skip_flg:bool = False
    # if 'url_pattern' in kwargs:   # url絞り込み指定あり
    if url_pattern:   # url絞り込み指定あり
        # pattern = re.compile(kwargs['url_pattern'])
        pattern = re.compile(url_pattern)
        if pattern.search(url) == None:
            skip_flg = True

    return skip_flg