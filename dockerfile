#FROM openjdk:11-stretch
FROM ubuntu:20.04

#LABELでメタ情報を入れることができる
#LABEL maintainer="Martijn Koster \"mak-docker@greenhills.co.uk\""
#LABEL repository="https://github.com/docker-solr/docker-solr"
LABEL maintainer="BrownieAtelier-test"

# CLI 上から引数を代入するキーを指定するコマンド。
# ARG <key> といった感じに書いておいて、 --build-arg <key>=<value> オプションで代入することで、
# Dockerfile 内で の値は使い回しすることができる。
# 要は、dockerコマンドとdockerfile間で情報をやり取りするための変数を定義しているようだ。

# OSユーザー＆パスワード
ARG OS_USER
ARG OS_PASS
ARG OS_ROOT_PASS
# アプリのログイン要素
ARG MONGO_SERVER
ARG MONGO_PORT
ARG MONGO_USE_DB
ARG MONGO_USER
ARG MONGO_PASS
ARG EMAIL_FROM
ARG EMAIL_TO
ARG EMAIL_PASS
ARG PRECECT_AUTH
ARG APP_PATH

# こんな感じでbuildコマンドで渡した引数をコンテナに動的に設定できそう。
# mongoDBへのアクセス情報
ENV MONGO_SERVER ${MONGO_SERVER}
ENV MONGO_PORT ${MONGO_PORT}
ENV MONGO_USE_DB ${MONGO_USE_DB}
ENV MONGO_USER ${MONGO_USER}
ENV MONGO_PASS ${MONGO_PASS}
# Email情報
ENV EMAIL_FROM ${EMAIL_FROM}
ENV EMAIL_TO ${EMAIL_TO}
ENV EMAIL_PASS ${EMAIL_PASS}
# Prefect情報
ENV PRECECT_AUTH ${PRECECT_AUTH}
# solr用の情報は後日実装するときに。

# mongoDB関連：環境変数設定
ENV MONGO_CRAWLER_RESPONSE "crawler_response"
ENV MONGO_CONTROLLER "controller"
ENV MONGO_CRAWLER_LOGS "crawler_logs"
ENV MONGO_SCRAPED_FROM_RESPONSE "scraped_from_response"
ENV MONGO_NEWS_CLIP_MASTER "news_clip_master"
ENV MONGO_SCRAPER_BY_DOMAIN "scraper_by_domain"
ENV MONGO_ASYNCHRONOUS_REPORT "asynchronous_report"
ENV MONGO_STATS_INFO_COLLECT "stats_info_collect"
# Prefectのログ設定情報
ENV PREFECT__LOGGING__LEVEL "INFO"
ENV PREFECT__LOGGING__FORMAT "%(asctime)s %(levelname)s [%(name)s] : %(message)s"
ENV PREFECT__LOGGING__DATEFMT "%Y-%m-%d %H:%M:%S"

# タイムゾーン設定
ENV TZ Asia/Tokyo
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# リポジトリ一覧を更新、インストール済みのパッケージ更新
RUN apt update
RUN apt -y upgrade
# RUN apt -y install build-essential curl git zlib1g-dev libssl-dev libbz2-dev libreadline-dev libsqlite3-dev wget vim systemd
RUN apt -y install build-essential libssl-dev wget


# Selenium用にgeckodriverを取得して配置
RUN wget -P /usr/local/bin/ "https://github.com/mozilla/geckodriver/releases/download/v0.31.0/geckodriver-v0.31.0-linux64.tar.gz"
RUN tar -zxvf /usr/local/bin/geckodriver-v0.31.0-linux64.tar.gz

# pythonまわりのインストール
RUN apt -y install software-properties-common
RUN add-apt-repository ppa:deadsnakes/ppa
RUN apt -y install python3.10
RUN apt -y install python3.10-venv
RUN apt -y install python3-pip
# 完了後PPAの削除
RUN add-apt-repository --remove ppa:deadsnakes/ppa

# rootユーザパスワード
RUN echo 'root:'${OS_ROOT_PASS} | chpasswd
# 新規グループ・ユーザーを作成。
RUN groupadd -g 1000 ${OS_USER}
RUN useradd  -g      ${OS_USER} -G sudo -m -s /bin/bash ${OS_USER}
# ユーザパスワード
RUN echo ${OS_USER}':'${OS_PASS} | chpasswd
#sudoユーザーを追加
# RUN echo 'Defaults visiblepw' >> /etc/sudoers
# RUN echo 'iganari ALL=(ALL) NOPASSWD:ALL' >> /etc/sud


WORKDIR /
RUN ls -l
# アプリ用ディレクトリ作成
WORKDIR /app

# ホストOSからアプリをコピー
RUN echo ${APP_PATH}
COPY ${APP_PATH} /app/
RUN pwd
RUN ls -l

# アプリ内でpython仮想環境構築
WORKDIR /app
RUN python3.10 -m venv .venv
RUN . .venv/bin/activate
RUN pip install -r requirements.txt

#VOLUME 命令は指定した名前でマウントポイントを作成し、他のホストやコンテナから外部マウント可能なボリュームにします
# VOLUME /app/data_dir

# ユーザー変更
USER ${OS_USER}

# 更新時に使われたが、その後不要となったものの一括削除
RUN apt autoremove
# キャッシュされているがインストールされていないdebファイルを削除
RUN apt autoclean

ENTRYPOINT []

# コンテナ起動時に以下のシェルを実行
CMD ["bash","./container_start.sh"]
