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
    userDataId = scrapy.Field()
    userLinkId = scrapy.Field()
    userImgUrl = scrapy.Field()
    userName = scrapy.Field()
    userFollowerCount = scrapy.Field()
    userAskCount = scrapy.Field()
    userAnswerCount = scrapy.Field()
    userUpCount = scrapy.Field()
