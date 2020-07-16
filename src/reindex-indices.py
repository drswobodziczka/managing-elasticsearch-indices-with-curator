import boto3
from requests_aws4auth import AWS4Auth
from elasticsearch import Elasticsearch, RequestsHttpConnection
import curator

host = 'vpc-ew1-psb-ci-elasticsearch-zc6rkopuyypw3eou2carvu47c4.eu-west-1.es.amazonaws.com'
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

new_index_version= '000001'

# Lambda execution starts here.
def list_indices(unit='days', unit_count=100):
  # list indices
  index_list = curator.IndexList(es)
  # Filters by naming prefix.
  index_list.filter_by_regex(kind='prefix', value='security-events-alerts')
  # Filters by age, anything with a time stamp older than 30 days in the index name.
  index_list.filter_by_age(source='name', direction='older', timestring='%Y-%m-%d', unit=unit, unit_count=unit_count)
  return index_list

def reindex(index_list, version_suffix):
  old_indices = index_list.indices

  print(f"{len(old_indices)} indices going to be reindexed: {old_indices}")
  for old_index in old_indices:
    new_index = f"{old_index}_{version_suffix}"
    # create -- when this may fail? -- a) index already exist b) connection error c) authentication error d) ...
    print(f"creating new index: {new_index}")
    # TODO: dry run mode -> real action
    curator.CreateIndex(es, f"{old_index}_{version_suffix}").do_dry_run()
    # reindex
    request_body = {
      "source": {
        "index": old_index
      },
      "dest": {
        "index": new_index
      }
    }

    # -- when this may fail? -- a) dest index already exist b) source index does not exist c) connection error d) authentication error e) ...
    print(f"sending reindex request: {request_body}")
    # TODO: dry run mode -> real action
    curator.Reindex(index_list, request_body, refresh=True, wait_for_completion=True).do_dry_run()

def assert_reindex_successfull(index_list, version_suffix):
  # TODO named touples here
  pass


# main
indices = list_indices()
reindex(index_list=indices, version_suffix=new_index_version)
assert_reindex_successfull(index_list=indices, version_suffix=new_index_version)

################ ! handle:

# ConnectionError(HTTPSConnectionPool
# Caused by NewConnectionError('<urllib3.connection.VerifiedHTTPSConnection object at 0x000001DB87C57F28>: Failed to establish a new connection
# FailedExecution
