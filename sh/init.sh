#!/bin/bash

whoami

# 下記のディレクトリにローカルGitリポジトリを作成する。
cd $HOME/BrownieAtelier
git init

# azureの共有ファイルに保存する際、セキュリティで警告が出るため以下のコマンドで安全であるディレクトリであると設定する。
git config --global --add safe.directory $HOME/BrownieAtelier
# BrownieAtelierのリモートリポジトリを登録
# git remote add origin https://github.com/pubranko/HatsuneMiku3.git
git remote add origin $GIT_REMOTE_REPOSITORY
# リモートリポジトリよりpullを実行し初回のソースを取得する。
git pull origin develop #まだmasterじゃない、、、
wait $!
