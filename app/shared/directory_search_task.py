import os
import sys
import re
import glob
import importlib
import inspect
from typing import Any, List, Final
path = os.getcwd()
sys.path.append(path)
from shared import settings


def directory_search_task(directory_path: str = settings.PREFECT_LIB__TASK_DIR) -> List[dict]:
    '''
    引数に渡されたパス内にあるタスククラスを読み込み、クラス名のリストにして返す。
    '''
    class_list: List[dict] = []
    # 引数で渡されたパス内のpythonモジュールファイル名を取得し、パス＋ファイル名を生成する。(fpath)
    # 拡張子を除去＆ファイルパスのセパレータ (Unix系なら'/') を '.' に置き換え
    # モジュールとして読み込み
    for file_path in glob.glob(os.path.join(directory_path, '*.py')):
        mod_path = os.path.splitext(file_path)[0].replace(os.path.sep, '.')
        mod = importlib.import_module(mod_path)

        # 読み込んだモジュールに含まれるクラス名とクラスインスタンスを呼び出す
        for class_name, _ in inspect.getmembers(mod, inspect.isclass):

            # 末尾がTaskのクラスに限定する。ただし継承用のクラスは除外する。
            ptn = re.compile(r'Task$')  #
            exclusion_list: list = ['ExtensionsTask', 'Task']
            select_flg: bool = True
            if ptn.search(class_name) and class_name not in exclusion_list:
                select_flg = True
            else:
                select_flg = False

            # 対象のクラスの場合、クラス名のdictを生成
            if select_flg:
                class_list.append({'class_name': class_name, })

        del mod  # 不要になったモジュールを削除(メモリ節約)
    return class_list


if __name__ == "__main__":
    # execute only if run as a script
    class_list = directory_search_task(settings.PREFECT_LIB__TASK_DIR)
    from pprint import pprint
    pprint(class_list)
