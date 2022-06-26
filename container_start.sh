# アプリディレクトリへ
cd /app
# python仮想環境を有効化
. .venv/bin/activate
# prefectクラウドへのログイン
prefect auth login --key $PRECECT_AUTH
# コンテナ内よりフローをprefectクラウドへ登録
python prefect_lib/exec/@flow_register.py
# prefectのバックエンドの向き先をクラウドへ
prefect backend cloud

# prefectエージェントを起動
# ※-fはログをコンソールに出力するオプション。通常不要だがデバック用に付与。
#prefect agent local start -l crawler-container
prefect agent local start -l crawler-container -f