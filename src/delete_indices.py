import logging
import boto3
import curator
from elasticsearch import Elasticsearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth

logging.basicConfig(filename='delete_indices.log', filemode='w', level=logging.INFO)
logger = logging.getLogger('IndexRemover')

class IndexRemover:
  host = ''
  region = 'eu-west-1'
  service = 'es'

  def __init__(self):
    credentials = boto3.Session(profile_name='saml').get_credentials()
    awsauth = AWS4Auth(credentials.access_key, credentials.secret_key,
                       IndexRemover.region, IndexRemover.service, session_token=credentials.token)

    # Build the Elasticsearch client.
    self.__es = Elasticsearch(
        hosts=[{'host': IndexRemover.host, 'port': 443}],
        http_auth=awsauth,
        use_ssl=True,
        verify_certs=True,
        connection_class=RequestsHttpConnection
    )

    logger.info(msg=f"established connection to Elastisearch instance: {self.__es}")

  def delete_indices(self, regex, dry_run=True):
    """
    @:arg regex: regular expression matching names of indices to be deleted
    @:arg dry_run: if `True` deletion process will go in "dry-run" mode i.e.:
    operations will be logged and NO index will be deleted. Defaults to 'True'
    """
    logger.info(msg=f"deletion regex: {regex}")

    indices = curator.IndexList(self.__es)
    indices.filter_by_regex(kind="regex", value=regex)
    self.__delete_index_list(index_list=indices, dry_run=dry_run)

  @staticmethod
  def __delete_index_list(index_list=None, dry_run=True):
    if dry_run:
      logger.info(msg=f"deleting indices: {index_list.indices}, mode: DRY-RUN")
      curator.DeleteIndices(index_list).do_dry_run()
    else:
      logger.info(msg=f"deleting indices: {index_list.indices}")
      curator.DeleteIndices(index_list).do_action()
