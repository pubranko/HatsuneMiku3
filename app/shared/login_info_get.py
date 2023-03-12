import yaml
import os
from typing import Any
from shared import settings


def login_info_get(path: str = settings.DATA_DIR__LOGIN_INFO, file: str = settings.DATA_DIR__LOGIN_INFO_YML) -> Any:
    '''
    クロール先のサイトへログインが必要な場合、必要なログイン情報を指定ファイルより取得する。
    デフォルトのファイル => 'data_dir/login_info/login_info.yml'
    '''
    full_path = os.path.join(path, file)
    try:
        with open(full_path, 'r') as yml:
            config: Any = yaml.safe_load(yml)
    except Exception as e:
        #raise Exception(f'指定したYAMLファイルがありません。path = {path}, file = {file}')
        pass
    else:
        return config


if __name__ == '__main__':
    file = login_info_get(path=settings.DATA_DIR__LOGIN_INFO, file = settings.DATA_DIR__LOGIN_INFO_YML)
    print(file)
    print(file['epochtimes.jp'])
    print(file['epochtimes.jp']['user'])
    print(file['epochtimes.jp']['password'])

    try:
        file['存在しないkey']
    except Exception as e:
        print('except ', e)  # => except  '存在しないkey'
