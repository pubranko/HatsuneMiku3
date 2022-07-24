import scrapy
from typing import Any
#from pymongo import MongoClient
from pymongo.mongo_client import MongoClient
import pickle
from bs4 import BeautifulSoup as bs4
from dateutil import parser
from models.mongo_model import MongoModel
from models.crawler_response_model import CrawlerResponseModel

mongo = MongoModel()
crawler_response = CrawlerResponseModel(mongo)

rec:Any = crawler_response.find_one(
    {'url': 'https://www.sankei.com/article/20210925-P5S53DNIGZPR5IBRCMVY77IEBU/'},
)

response_body = pickle.loads(rec['response_body'])

print(response_body)