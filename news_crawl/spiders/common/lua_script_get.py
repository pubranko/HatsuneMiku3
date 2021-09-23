import os


def lua_script_get(script_name: str) -> str:
    '''
    '''
    path = os.path.join(
        'news_crawl', 'spiders', 'lua_script', script_name + '.lua')
    with open(path, 'r') as file:
        return file.read()
