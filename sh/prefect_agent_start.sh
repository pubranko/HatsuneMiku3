#!/bin/bash
# アプリディレクトリへ
cd $HOME/BrownieAtelier
# python仮想環境を有効化
. $HOME/.venv/bin/activate
# prefectクラウドへのログイン
$HOME/.venv/bin/prefect auth login --key $PRECECT_AUTH
# コンテナ内よりフローをprefectクラウドへ登録
# python prefect_lib/exec/@flow_register.py
# prefectのバックエンドの向き先をクラウドへ
$HOME/.venv/bin/prefect backend cloud

# prefectエージェントを起動
#   -l -> labelsの指定。Flow登録時のlabelを指定することによりFlowの実行が可能となる。登録されているFlowのlabel(複数設定可)と合わない場合、実行できない。
#         複数のエージェントを同時に起動する場合、一意となるラベル名も追加で付与すること。そうしないと競合する。
#   -f -> ログをコンソールに出力するオプション。通常不要だがデバック用に付与。
#   --no-hostname-label -> エージェント起動時にlabelsには、-lで指定した値とホスト名が付与される。
#                          このホスト名の部分を無しにできるオプション。
#                          この端末専用のフロー以外拒否したい場合はこのオプションを外してください。
#prefect agent local start -l crawler-container
$HOME/.venv/bin/prefect agent local start -l crawler-container -f --no-hostname-label