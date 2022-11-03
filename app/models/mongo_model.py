#from pymongo import MongoClient
from pymongo.mongo_client import MongoClient
from pymongo.database import Database
import os
from urllib.parse import quote_plus

class MongoModel(object):
    '''
    MongoDB用モデル
    '''
    __mongo_server: str
    __mongo_port: str
    __mongo_db_name: str
    __mongo_user: str
    __mongo_pass: str
    __mongo_client: MongoClient
    mongo_db: Database

    def __init__(self):
        self.__mongo_server = os.environ['MONGO_SERVER']
        self.__mongo_port = os.environ['MONGO_PORT']
        self.__mongo_db_name = os.environ['MONGO_USE_DB']
        self.__mongo_user = os.environ['MONGO_USER']
        self.__mongo_pass = os.environ['MONGO_PASS']
        self.__mongo_tls = os.environ['MONGO_TLS']
        self.__mongo_tls_ca_certs = os.environ['MONGO_TLS_CA_FILE']
        self.__mongo_tls_certtificate_key_file = os.environ['MONGO_TLS_CERTTIFICATE_KEY_FILE']


        # uri = f'mongodb://{quote_plus(self.__mongo_user)}:{quote_plus(self.__mongo_pass)}@{self.__mongo_server}/{self.__mongo_db_name}?'
        # uri = f'mongodb://{quote_plus(self.__mongo_user)}:{quote_plus(self.__mongo_pass)}@{self.__mongo_server}:{self.__mongo_port}/{self.__mongo_db_name}?'
        # if self.__mongo_tls == 'true':
        #     uri = f'{uri}tls=true&tlsCAFile={self.__mongo_tls_ca_certs}&tlsCertificateKeyFile={self.__mongo_tls_certtificate_key_file}'
        # self.__mongo_client = MongoClient(
        #     host=uri, #port=int(self.__mongo_port),
        # )

        # self.__mongo_client = MongoClient(
        #     host=self.__mongo_server,
        #     port=int(self.__mongo_port),
        #     username=self.__mongo_user,
        #     password=self.__mongo_pass,
        #     authSource=self.__mongo_db_name,
        #     tls=True if self.__mongo_tls else False,
        #     tlsCAFile=self.__mongo_tls_ca_certs  if self.__mongo_tls else None,
        #     tlsCertificateKeyFile=self.__mongo_tls_certtificate_key_file  if self.__mongo_tls else None,
        # )
        # MongoClient(
        #     host: str | Sequence[str] | None = None,
        #     port: int | None = None,
        #     document_class: Unknown | None = None,
        #     tz_aware: bool | None = None,
        #     connect: bool | None = None,
        #     type_registry: TypeRegistry | None = None,
        #     **kwargs: Any)
        param:dict = {
                # 'host': quote_plus(self.__mongo_server),
                'host': self.__mongo_server,
                'port': int(self.__mongo_port),
                'username': self.__mongo_user,
                'password': self.__mongo_pass,
                'authSource': self.__mongo_db_name,     # ユーザー認証を行うDB
        }
        # tls認証を行う場合、以下のパラメータを追加
        if self.__mongo_tls=='true':
            param.update({
                'tls': True,
                'tlsCAFile': self.__mongo_tls_ca_certs,
                'tlsCertificateKeyFile': self.__mongo_tls_certtificate_key_file,
                # 'directConnection': True,
            })

        self.__mongo_client = MongoClient(**param)
        self.mongo_db = self.__mongo_client[self.__mongo_db_name]

    def close(self):
        '''
        MongoClientを閉じる。
        '''
        self.__mongo_client.close()