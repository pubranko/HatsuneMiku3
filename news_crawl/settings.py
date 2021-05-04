# Scrapy settings for news_crawl project
#
# For simplicity, this file contains only settings considered important or
# commonly used. You can find more settings consulting the documentation:
#
#     https://docs.scrapy.org/en/latest/topics/settings.html
#     https://docs.scrapy.org/en/latest/topics/downloader-middleware.html
#     https://docs.scrapy.org/en/latest/topics/spider-middleware.html

BOT_NAME = 'news_crawl'

SPIDER_MODULES = ['news_crawl.spiders']
NEWSPIDER_MODULE = 'news_crawl.spiders'

# Crawl responsibly by identifying yourself (and your website) on the user-agent
# リクエストに含まれるユーザーエージェントの指定
#USER_AGENT = 'news_crawl (+http://www.yourdomain.com)'
USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:88.0) Gecko/20100101 Firefox/88.0'

# Obey robots.txt rules
ROBOTSTXT_OBEY = True

# Configure maximum concurrent requests performed by Scrapy (default: 16)
# 同時平行処理するリクエストの最大値
#CONCURRENT_REQUESTS = 32

# Configure a delay for requests for the same website (default: 0)
# See https://docs.scrapy.org/en/latest/topics/settings.html#download-delay
# See also autothrottle settings and docs
#DOWNLOAD_DELAY = 3
DOWNLOAD_DELAY = 1.5
# The download delay setting will honor only one of:
# webサイトのドメインごとに、同時平行処理するリクエストの最大値
#CONCURRENT_REQUESTS_PER_DOMAIN = 16
# webサイトのIPごとの同時並行リクエストの最大値。これを指定すると、DOWNLOAD_DELAYもipごとになり、CONCURRENT_REQUESTS_PER_DOMAINは無視される。
#CONCURRENT_REQUESTS_PER_IP = 16

# Disable cookies (enabled by default)
# Cookieを有効にするかどうか。
#COOKIES_ENABLED = False

# Disable Telnet Console (enabled by default)
#TELNETCONSOLE_ENABLED = False

# Override the default request headers:
# リクエストにデフォルトで含めるヘッダーをdictで指定する。
#DEFAULT_REQUEST_HEADERS = {
#   'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
#   'Accept-Language': 'en',
#}

# Enable or disable spider middlewares
# See https://docs.scrapy.org/en/latest/topics/spider-middleware.html
# スパイダーのミドルウェアを作る場合に使用する。
#SPIDER_MIDDLEWARES = {
#    'news_crawl.middlewares.NewsCrawlSpiderMiddleware': 543,
#}

# Enable or disable downloader middlewares
# See https://docs.scrapy.org/en/latest/topics/downloader-middleware.html
# ダウンロードのミドルウェアを自作のものを使いたい場合、以下の設定を変える。
#DOWNLOADER_MIDDLEWARES = {
#    'news_crawl.middlewares.NewsCrawlDownloaderMiddleware': 543,
#}

# Enable or disable extensions
# See https://docs.scrapy.org/en/latest/topics/extensions.html
#'<プロジェクト名><ファイル名><クラス名>:優先度
#EXTENSIONS = {
#    'scrapy.extensions.telnet.TelnetConsole': None,
#}

# Configure item pipelines
# See https://docs.scrapy.org/en/latest/topics/item-pipeline.html
#アイテムのパイプラインの設定
#ITEM_PIPELINES = {
#    'news_crawl.pipelines.NewsCrawlPipeline': 300,
#}
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
HTTPCACHE_ENABLED = True
# 上記でキャッシュを有効にした場合、有効な秒数を指定。0は無限。 900秒→15分、3600→1時間、86400→1日
HTTPCACHE_EXPIRATION_SECS = 60

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
#DEPTH_LIMIT = 2
#DEPTH_STATS_VERBOSE = True

#何かしら時間による処理を行いたい場合、使用するタイムゾーンを定義する。
#  例：spider内のsitemap_fillterで、lastmodの時間を絞り込みしたい。引数に与える時間のタイムゾーンには、settingsのTIME_ZONEを使用する。
from datetime  import timedelta,timezone
TIMEZONE = timezone(timedelta(hours=9), 'JST')

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
    'scrapy.downloadermiddlewares.stats.DownloaderStats': 850,
    'scrapy.downloadermiddlewares.httpcache.HttpCacheMiddleware': 900,
}
'''

