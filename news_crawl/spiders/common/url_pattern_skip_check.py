import re


def url_pattern_skip_check(url: str, kwargs: dict):
    crwal_flg:bool = False
    if 'url_pattern' in kwargs:   # url絞り込み指定あり
        pattern = re.compile(kwargs['url_pattern'])
        if pattern.search(url) == None:
            crwal_flg = True

    return crwal_flg