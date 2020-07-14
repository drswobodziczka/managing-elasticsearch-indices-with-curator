import boto3
import curator
import logging
from requests_aws4auth import AWS4Auth
from elasticsearch import Elasticsearch, RequestsHttpConnection

host = ''
region = 'eu-west-1'
service = 'es'
credentials = boto3.Session(profile_name='saml').get_credentials()
awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service, session_token=credentials.token)

# Build the Elasticsearch client.
es = Elasticsearch(
  hosts=[{'host': host, 'port': 443}],
  http_auth=awsauth,
  use_ssl=True,
  verify_certs=True,
  connection_class=RequestsHttpConnection
)

logging.basicConfig(filename='delete_indices.log', filemode='w', level=logging.INFO)

def delete_indices(regex, dry_run=True):
  index_list = curator.IndexList(es)
  index_list.filter_by_regex(kind="regex", value=regex)
  delete_index_list(index_list=index_list, dry_run=dry_run)

def delete_index_list(index_list, dry_run=True):
  if dry_run:
    curator.DeleteIndices(index_list).do_dry_run()
  else:
    curator.DeleteIndices(index_list).do_action()


delete_indices(regex="_0000", dry_run=False)