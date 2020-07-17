import boto3
from elasticsearch import Elasticsearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth

class ElasticsearchClient:

  @staticmethod
  def connect(host: str = '',
              region: str = 'eu-west-1', service: str = 'es') -> Elasticsearch:
    credentials = boto3.Session(profile_name='saml').get_credentials()
    awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service, session_token=credentials.token)
    elasticsearch = Elasticsearch(hosts=[{'host': host, 'port': 443}], http_auth=awsauth, use_ssl=True,
                                  verify_certs=True, connection_class=RequestsHttpConnection)
    return elasticsearch
