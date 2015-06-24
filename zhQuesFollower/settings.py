# -*- coding: utf-8 -*-

# Scrapy settings for zhQuesFollower project
#
# For simplicity, this file contains only the most important settings by
# default. All the other settings are documented here:
#
#     http://doc.scrapy.org/en/latest/topics/settings.html
#

BOT_NAME = 'zhQuesFollower'

SPIDER_MODULES = ['zhQuesFollower.spiders']
NEWSPIDER_MODULE = 'zhQuesFollower.spiders'

DOWNLOAD_TIMEOUT = 700

# CONCURRENT_REQUESTS = 70
# CONCURRENT_REQUESTS_PER_DOMAIN = 70

LOG_LEVEL = 'INFO'

# Crawl responsibly by identifying yourself (and your website) on the user-agent
#USER_AGENT = 'zhQuesFollower (+http://www.yourdomain.com)'



DEFAULT_REQUEST_HEADERS = {
           'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
           'Accept-Encoding': 'gzip, deflate, sdch',
           'Accept-Language': 'zh-CN,zh;q=0.8,en-US;q=0.6,en;q=0.4,zh-TW;q=0.2',
           'Connection': 'keep-alive',
           'Host': 'www.zhihu.com',
           'Referer': 'http://www.zhihu.com/',

}

USER_AGENT = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2311.135 Safari/537.36'
#
# CACHE_SERVER_1 = 'd69c4508ccc94dc4.m.cnbjalinu16pub001.ocs.aliyuncs.com:11211'
# CACHE_USER_1 = 'd69c4508ccc94dc4'
# CACHE_PASSWORD_1 = 'Zhihucache1'
#
# CACHE_SERVER_2 = '7030b81da1324743.m.cnbjalinu16pub001.ocs.aliyuncs.com:11211'
# CACHE_USER_2 = '7030b81da1324743'
# CACHE_PASSWORD_2 = 'Zhihucache2'
#
# CACHE_SERVER_3 = '92a2b309a9f145d2.m.cnbjalinu16pub001.ocs.aliyuncs.com:11211'
# CACHE_USER_3 = '92a2b309a9f145d2'
# CACHE_PASSWORD_3 = 'Zhihucache3'
#
# CACHE_SERVER_71 = 'aa41ddf13b914084.m.cnbjalinu16pub001.ocs.aliyuncs.com:11211'
# CACHE_USER_71 = 'aa41ddf13b914084'
# CACHE_PASSWORD_71 = 'Zhihu7771'
#
# CACHE_SERVER_72 = 'b2954ece3d1647b8.m.cnbjalinu16pub001.ocs.aliyuncs.com:11211'
# CACHE_USER_72 = 'b2954ece3d1647b8'
# CACHE_PASSWORD_72 = 'Zhihu7772'
UPDATE_PERIOD = '432000' #最快5天更新一次
REDIS_HOST = 'f57567e905c811e5.m.cnbja.kvstore.aliyuncs.com'
REDIS_PORT = '6379'
REDIS_USER = 'f57567e905c811e5'
REDIS_PASSWORD = 'Zhihu777r'

HBASE_HOST='localhost'


SCRAPYD_HOST_LIST=[
    '192.168.1.1'
    ,'192.168.1.1'
    ,'192.168.1.1'
    ,'192.168.1.1'
    ,'192.168.1.1'
    ,'192.168.1.1'
]
SCRAPYD_PORT='6800'

EMAIL_LIST=[
    'h1@1.com'
    ,'h2@1.com'
    ,'h2@1.com'
    ,'h2@1.com'
    ,'h3@1.com'
    ,'h4@1.com']
PASSWORD_LIST=[
    'h1'
    ,'h2'
    ,'h2'
    ,'h2'
    ,'h3'
    ,'h4'
]


APP_ID = '6f72qbnhmko93jjnlspg2fhivs1shftog67gqlkfe6kqv3lb'
MASTER_KEY = '53jugkhuvrbkgwkutnlflwiy55skhaxecau4teya0gc5b7f9'

ITEM_PIPELINES = {
    'zhQuesFollower.pipelines.FollowerPipeline': 300,
   # 'zhihut.pipelines.SecondPipline': 800,
}
SPIDER_MIDDDLEWARES = {
    'scrapy.contrib.spidermiddleware.httperror.HttpErrorMiddleware':300,
}

DUPEFILTER_CLASS = 'zhQuesFollower.custom_filters.SeenURLFilter'