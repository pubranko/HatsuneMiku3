FROM ubuntu:20.04

#LABELでメタ情報を入れることができる
LABEL maintainer="BrownieAtelier-base-test"

# CLI 上から引数を代入するキーを指定するコマンド。
# ARG <key> といった感じに書いておいて、 --build-arg <key>=<value> オプションで代入することで、
# Dockerfile 内で の値は使い回しすることができる。
# 要は、dockerコマンドとdockerfile間で情報をやり取りするための変数を定義しているようだ。
# OSユーザー＆パスワード
ARG OS_USER
ARG OS_PASS
ARG OS_ROOT_PASS

# タイムゾーン設定
ENV TZ Asia/Tokyo
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# リポジトリ一覧を更新、インストール済みのパッケージ更新
# 必要なアプリをインストール
RUN apt-get update
RUN apt update
RUN apt -y upgrade
RUN apt -y install build-essential libssl-dev wget firefox git

# Selenium用にgeckodriverを取得して配置。不要になったダウンロードファイル削除
RUN wget -P /usr/local/bin/ "https://github.com/mozilla/geckodriver/releases/download/v0.31.0/geckodriver-v0.31.0-linux64.tar.gz"
RUN tar -zxvf /usr/local/bin/geckodriver-v0.31.0-linux64.tar.gz -C /usr/local/bin
RUN rm /usr/local/bin/geckodriver-v0.31.0-linux64.tar.gz

RUN apt -y install python3.9
RUN apt -y install python3.9-venv
RUN apt -y install python3-selenium

# 更新時に使われたが、その後不要となったものの一括削除
# キャッシュされているがインストールされていないdebファイルを削除
# 不要なアプリを削除
RUN apt autoremove
RUN apt autoclean
RUN apt -y remove build-essential libssl-dev wget

# rootユーザパスワード
RUN echo 'root:'${OS_ROOT_PASS} | chpasswd
# 新規グループ・ユーザーを作成。
RUN groupadd -g 1000 ${OS_USER}
RUN useradd  -g      ${OS_USER} -G sudo -m -s /bin/bash ${OS_USER}
# ユーザパスワード
RUN echo ${OS_USER}':'${OS_PASS} | chpasswd
#sudoユーザーを追加
RUN echo 'Defaults visiblepw' >> /etc/sudoers
RUN echo ${OS_USER}' ALL=(ALL) NOPASSWD:ALL' >> /etc/sud
# rootユーザーログイン無効化
# RUN passwd -l root

USER ${OS_USER}
RUN mkdir ~/BrownieAtelier

RUN git init

# azureの共有ファイルに保存する際、セキュリティで警告が出るため以下のコマンドで安全であるディレクトリであると設定する。
RUN git config --global --add safe.directory /home/${OS_USER}/BrownieAtelier
# BrownieAtelierのリモートリポジトリを登録
# git remote add origin https://github.com/pubranko/HatsuneMiku3.git
RUN git remote add origin $GIT_REMOTE_REPOSITORY
# リモートリポジトリよりpullを実行し初回のソースを取得する。
#まだmasterじゃない、、、
RUN git pull origin develop



# COPY --chownは、作成されたすべてのディレクトリの所有者を変更しません
COPY --chown=0:0 sh/init.sh /home/${OS_USER}


ENTRYPOINT []
CMD []
