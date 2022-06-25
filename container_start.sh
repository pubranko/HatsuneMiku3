cd /app
. .venv/bin/activate

prefect auth login --key 27-LM_dqhA58Y95wy4Gb3w
prefect backend cloud
prefect agent local start