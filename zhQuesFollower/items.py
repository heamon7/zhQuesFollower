# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class ZhquesfollowerItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    offset = scrapy.Field()
    questionId = scrapy.Field()
    userDataIdList = scrapy.Field()
    userLinkList = scrapy.Field()
    userImgUrlList = scrapy.Field()
    userNameList = scrapy.Field()
    userFollowersList = scrapy.Field()
    userAskList = scrapy.Field()
    userAnswerList = scrapy.Field()
    userUpList = scrapy.Field()
