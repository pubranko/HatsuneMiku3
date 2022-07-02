# Scrapy settings for news_crawl project
#
# For simplicity, this file contains only settings considered important or
# commonly used. You can find more settings consulting the documentation:
#
#     https://docs.scrapy.org/en/latest/topics/settings.html
#     https://docs.scrapy.org/en/latest/topics/downloader-middleware.html
#     https://docs.scrapy.org/en/latest/topics/spider-middleware.html

# デフォルトの設定はこちらにあるようだ。
# ~.venv/lib/python3.8/site-packages/scrapy/settings/default_settings.py
import os
from datetime import timedelta, timezone
from shutil import which
BOT_NAME = 'news_crawl'

SPIDER_MODULES = ['news_crawl.spiders']
NEWSPIDER_MODULE = 'news_crawl.spiders'

# Crawl responsibly by identifying yourself (and your website) on the user-agent
# リクエストに含まれるユーザーエージェントの指定
#USER_AGENT = 'news_crawl (+http://www.yourdomain.com)'
USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:88.0) Gecko/20100101 Firefox/88.0'

# Obey robots.txt rules
ROBOTSTXT_OBEY = True
#ROBOTSTXT_OBEY = False

# Configure maximum concurrent requests performed by Scrapy (default: 16)
# 同時平行処理するリクエストの最大値
#CONCURRENT_REQUESTS = 32
CONCURRENT_REQUESTS = 100

# Configure a delay for requests for the same website (default: 0)
# See https://docs.scrapy.org/en/latest/topics/settings.html#download-delay
# See also autothrottle settings and docs
#DOWNLOAD_DELAY = 3
DOWNLOAD_DELAY = 3

# The download delay setting will honor only one of:
# webサイトのドメインごとに、同時平行処理するリクエストの最大値
#CONCURRENT_REQUESTS_PER_DOMAIN = 100
CONCURRENT_REQUESTS_PER_DOMAIN = 1
# webサイトのIPごとの同時並行リクエストの最大値。これを指定すると、DOWNLOAD_DELAYもipごとになり、CONCURRENT_REQUESTS_PER_DOMAINは無視される。
#CONCURRENT_REQUESTS_PER_IP = 1

# Disable cookies (enabled by default)
# Cookieを有効にするかどうか。
#COOKIES_ENABLED = False

# Disable Telnet Console (enabled by default)
TELNETCONSOLE_ENABLED = False
#telnet関係の設定は以下のとおり
#TELNETCONSOLE_HOST = localhost
#TELNETCONSOLE_PORT = [6023, 6073]
#TELNETCONSOLE_USERNAME = 'scrapy'
#TELNETCONSOLE_PASSWORD = scrapy実行時に動的に割当られる。

# Override the default request headers:
# リクエストにデフォルトで含めるヘッダーをdictで指定する。
# DEFAULT_REQUEST_HEADERS = {
#   'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
#   'Accept-Language': 'en',
# }

# Enable or disable spider middlewares
# See https://docs.scrapy.org/en/latest/topics/spider-middleware.html
# スパイダーのミドルウェアを作る場合に使用する。
SPIDER_MIDDLEWARES = {
    # 'news_crawl.middlewares.NewsCrawlSpiderMiddleware': 543,
    # Splash用
    'scrapy_splash.SplashDeduplicateArgsMiddleware': 100,
}

# Enable or disable downloader middlewares
# See https://docs.scrapy.org/en/latest/topics/downloader-middleware.html
# ダウンロードのミドルウェアを自作のものを使いたい場合、以下の設定を変える。
DOWNLOADER_MIDDLEWARES = {
    #'news_crawl.middlewares.NewsCrawlDownloaderMiddleware': 543,
    #splash用
    # 'scrapy_splash.SplashCookiesMiddleware': 723,
    # 'scrapy_splash.SplashMiddleware': 725,
    #selenium用 -> カスタムバージョン
    #'news_crawl.scrapy_selenium_custom_middlewares.SeleniumMiddleware': 800,
    #selenium用
    #'scrapy_selenium.SeleniumMiddleware': 800,

    # defalt_settings.pyより
    # Engine side
    # 'scrapy.downloadermiddlewares.robotstxt.RobotsTxtMiddleware': 100,
    # 'scrapy.downloadermiddlewares.httpauth.HttpAuthMiddleware': 300,
    # 'scrapy.downloadermiddlewares.downloadtimeout.DownloadTimeoutMiddleware': 350,
    # 'scrapy.downloadermiddlewares.defaultheaders.DefaultHeadersMiddleware': 400,
    # 'scrapy.downloadermiddlewares.useragent.UserAgentMiddleware': 500,
    # 'scrapy.downloadermiddlewares.retry.RetryMiddleware': 550,
    # 'scrapy.downloadermiddlewares.ajaxcrawl.AjaxCrawlMiddleware': 560,
    # 'scrapy.downloadermiddlewares.redirect.MetaRefreshMiddleware': 580,
    # 'scrapy.downloadermiddlewares.httpcompression.HttpCompressionMiddleware': 590,
    # 'scrapy.downloadermiddlewares.redirect.RedirectMiddleware': 600,
    # 'scrapy.downloadermiddlewares.cookies.CookiesMiddleware': 700,
    # 'scrapy.downloadermiddlewares.httpproxy.HttpProxyMiddleware': 750,
    # 'scrapy.downloadermiddlewares.stats.DownloaderStats': 850,
    # 'scrapy.downloadermiddlewares.httpcache.HttpCacheMiddleware': 900,
    # Downloader side
}

# Enable or disable extensions
# See https://docs.scrapy.org/en/latest/topics/extensions.html
# '<プロジェクト名><ファイル名><クラス名>:優先度
# EXTENSIONS = {
#    'scrapy.extensions.telnet.TelnetConsole': None,
# }

# Configure item pipelines
# See https://docs.scrapy.org/en/latest/topics/item-pipeline.html
# アイテムのパイプラインの設定
# ITEM_PIPELINES = {
#    'news_crawl.pipelines.NewsCrawlPipeline': 300,
# }
ITEM_PIPELINES = {
    'news_crawl.pipelines.MongoPipeline': 300,
}

# Enable and configure the AutoThrottle extension (disabled by default)
# See https://docs.scrapy.org/en/latest/topics/autothrottle.html
#AUTOTHROTTLE_ENABLED = True
# The initial download delay
#AUTOTHROTTLE_START_DELAY = 5
# The maximum download delay to be set in case of high latencies
#AUTOTHROTTLE_MAX_DELAY = 60
# The average number of requests Scrapy should be sending in parallel to
# each remote server
#AUTOTHROTTLE_TARGET_CONCURRENCY = 1.0
# Enable showing throttling stats for every response received:
#AUTOTHROTTLE_DEBUG = False

# Enable and configure HTTP caching (disabled by default)
# See https://docs.scrapy.org/en/latest/topics/downloader-middleware.html#httpcache-middleware-settings
# HTTPキャッシュを使うかどうかの指定。キャッシュを使うと、２回目以降はサーバーにリクエストが送られず、
# レスポンスがキャッシュから取得できる。
#HTTPCACHE_ENABLED = True
HTTPCACHE_ENABLED = False
# 上記でキャッシュを有効にした場合、有効な秒数を指定。0は無限。 900秒→15分、3600→1時間、86400→1日
HTTPCACHE_EXPIRATION_SECS = 900  # 3600

# フォルダ名だけ指定した場合、こうなる「〜/myproject/.scrapy/scrapy_httpcache」
# 絶対パスでの指定の場合：'/var/cache/ranko'
HTTPCACHE_DIR = 'httpcache'
# レスポンスをキャッシュしないHTTPステータスコード。
#HTTPCACHE_IGNORE_HTTP_CODES = []
# よくわからないが、ファイル自体のレスポンスに関する何か？
#HTTPCACHE_STORAGE = 'scrapy.extensions.httpcache.FilesystemCacheStorage'

##########################################
# 拡張してみた
##########################################
# 異なるドメインを並列にクロールさせるための設定
# 単一のドメインへ最適化された設定（デフォルト）
#SCHEDULER_PRIORITY_QUEUE = 'scrapy.pqueues.ScrapyPriorityQueue'
# 広範囲なドメインへ最適化された設定
SCHEDULER_PRIORITY_QUEUE = 'scrapy.pqueues.DownloaderAwarePriorityQueue'
REACTOR_THREADPOOL_MAXSIZE = 100
#AJAXCRAWL_ENABLED = True

#DEPTH_LIMIT = 2
#DEPTH_STATS_VERBOSE = True

# 何かしら時間による処理を行いたい場合、使用するタイムゾーンを定義する。
#  例：spider内のsitemap_fillterで、lastmodの時間を絞り込みしたい。引数に与える時間のタイムゾーンには、settingsのTIME_ZONEを使用する。
TIMEZONE = timezone(timedelta(hours=9), 'JST')

#LOGのレベル(CRITICAL > ERROR > WARNING > INFO > DEBUG)
#LOG_LEVEL = 'INFO'
LOG_LEVEL = 'DEBUG'
#LOG_FILE = 'logs/test.log'
#LOG_FILE = ''
# ロギングを有効にするかどうか。
LOG_ENABLED = True
#LOG_ENABLED = False
LOG_ENCODING = 'utf-8'
# ログ・メッセージをフォーマットするための文字列。 利用可能なプレース・ホルダーの全リストについては、 Python logging documentation を参照してください。
LOG_FORMAT = '%(asctime)s %(levelname)s [%(name)s] : %(message)s'
#LOG_FORMAT = '[%(asctime)s] %(levelname)s - %(name)s | %(message)s'
# 日付/時刻をフォーマットするための文字列、 LOG_FORMAT の %(asctime)s プレース・ホルダーの展開。
# 利用可能なディレクティブのリストについては、 Python datetime documentation を参照してください。
LOG_DATEFORMAT = '%Y-%m-%d %H:%M:%S'
#LOG_DATEFORMAT = '%Y-%m-%d %H:%M:%S%z'
#LOG_FORMATTER = True
# True の場合、処理のすべての標準出力(およびエラー)がログにリダイレクトされます。 たとえば、 print('hello') の場合、Scrapyログに表示されます。
#LOG_STDOUT = False
# True の場合、ログにはルート・パスのみが含まれます。 False に設定されている場合、ログ出力を担当するコンポーネントが表示されます
#LOG_SHORT_NAMES = False
# LogStats による統計の各ログ出力間の間隔(秒単位)。
#LOGSTATS_INTERVAL = 60.0
INSTALL_ROOT_HANDLER=False

# Scrapy-Seleniumの設定。上述のDOWNLOADER_MIDDLEWARES={}にも設定を行っている。
SELENIUM_DRIVER_NAME = 'firefox'
SELENIUM_DRIVER_EXECUTABLE_PATH = which('geckodriver')
SELENIUM_DRIVER_ARGUMENTS = ['-headless']
#SELENIUM_DRIVER_ARGUMENTS = []
# ブラウザ・通信の不可軽減のため、独自の設定を追加してみた。
#   その他の設定については、ここが参考になりそう https://www.programcreek.com/python/example/100026/selenium.webdriver.FirefoxProfile
#   1:通常、2:禁止
SELENIUM_DRIVER_SET_PREFERENCE = {
    'permissions.default.image': 2,                 # 画像のダウンロード禁止
    'permissions.default.image.animation_mode': 2,  # gitなどのアニメーションのダウンロード禁止
    'permissions.default.stylesheet': 2,            # cssのダウンロード禁止
}

# Scrapy-Splashの設定
SPLASH_URL = 'http://localhost:8050/'
DUPEFILTER_CLASS = 'scrapy_splash.SplashAwareDupeFilter'
HTTPCACHE_STORAGE = 'scrapy_splash.SplashAwareFSCacheStorage'

# splashの再起動でエラーが多発する。その分をretryでカバーしたい。
RETRY_ENABLED = True
#RETRY_TIMES = 2    requestオブジェクトで直接拡張させたのでここでの設定不要。
RETRY_HTTP_CODES = [500, 502, 503, 504, 522, 524, 408, 429]

# 排他制御用のワークディレクトリ設定
from common_lib.common_settings import DATA_DIR
EXCLUSIVE_WORK = os.path.join(DATA_DIR, 'exclusive_work')


'''デフォルトセッティングのパス
.venv/lib/python3.8/site-packages/scrapy/settings/default_settings.py
'''


'''公式よりミドルウェアの優先順
{
    'scrapy.downloadermiddlewares.robotstxt.RobotsTxtMiddleware': 100,
    'scrapy.downloadermiddlewares.httpauth.HttpAuthMiddleware': 300,
    'scrapy.downloadermiddlewares.downloadtimeout.DownloadTimeoutMiddleware': 350,
    'scrapy.downloadermiddlewares.defaultheaders.DefaultHeadersMiddleware': 400,
    'scrapy.downloadermiddlewares.useragent.UserAgentMiddleware': 500,
    'scrapy.downloadermiddlewares.retry.RetryMiddleware': 550,
    'scrapy.downloadermiddlewares.ajaxcrawl.AjaxCrawlMiddleware': 560,
    'scrapy.downloadermiddlewares.redirect.MetaRefreshMiddleware': 580,
    'scrapy.downloadermiddlewares.httpcompression.HttpCompressionMiddleware': 590,
    'scrapy.downloadermiddlewares.redirect.RedirectMiddleware': 600,
    'scrapy.downloadermiddlewares.cookies.CookiesMiddleware': 700,
    'scrapy.downloadermiddlewares.httpproxy.HttpProxyMiddleware': 750,
    ここにseleniumuが入るイメージ
    'scrapy.downloadermiddlewares.stats.DownloaderStats': 850,
    'scrapy.downloadermiddlewares.httpcache.HttpCacheMiddleware': 900,
}
'''
