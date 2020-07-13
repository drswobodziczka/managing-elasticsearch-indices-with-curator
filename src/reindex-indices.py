import boto3
from requests_aws4auth import AWS4Auth
from elasticsearch import Elasticsearch, RequestsHttpConnection
import curator

host = 'vpc-ew1-psb-ci-elasticsearch-zc6rkopuyypw3eou2carvu47c4.eu-west-1.es.amazonaws.com'
region = 'eu-west-1'
service = 'es'
credentials = boto3.Session(profile_name='saml').get_credentials()
awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service, session_token=credentials.token)

# Lambda execution starts here.
def reindex(indicesDict):

    # Build the Elasticsearch client.
    es = Elasticsearch(
        hosts = [{'host': host, 'port': 443}],
        http_auth = awsauth,
        use_ssl = True,
        verify_certs = True,
        connection_class = RequestsHttpConnection
    )

    # list indices
    index_list = curator.IndexList(es)

    # Filters by age, anything with a time stamp older than 30 days in the index name.
    # index_list.filter_by_age(source='name', direction='older', timestring='%Y.%m.%d', unit='days', unit_count=30)

    # Filters by naming prefix.
    # index_list.filter_by_regex(kind='prefix', value='my-logs-2017')

    # Filters by age, anything created more than one month ago.
    # index_list.filter_by_age(source='creation_date', direction='older', unit='months', unit_count=1)

    print(f"Found {len(index_list.indices)} indices to reindex")
    print(f"indices: {index_list.indices}")
    print(f"index info: {index_list.index_info}")
    print(f"all indices: {index_list.all_indices}")



reindex(None)
