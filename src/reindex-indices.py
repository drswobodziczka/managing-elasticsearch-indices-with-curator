import logging

from curator import IndexList, CreateIndex, Reindex

from src.ElasticsearchClient import ElasticsearchClient

logging.basicConfig(filename='delete_indices.log',
                    filemode='w',
                    level=logging.INFO)
logger = logging.getLogger('Reindexer')


class Reindexer:

  def __init__(self) -> None:
    self.__es = ElasticsearchClient.connect()

  def reindex(self, version_suffix: str, dry_run: bool = True):
    index_list = self.__list_indices(unit='days', unit_count=1)
    old_indices = index_list.indices
    print(f"{len(old_indices)} indices going to be reindexed..")

    for old_index in old_indices:
      new_index = f"{old_index}_{version_suffix}"
      self.__create_index(dry_run, new_index)
      self.reindex_single(index_list, new_index, old_index, dry_run)

  def __list_indices(self, unit: str, unit_count: int) -> IndexList:
    index_list = IndexList(self.__es)
    index_list.filter_by_regex(kind='prefix', value='security-events-alerts')
    index_list.filter_by_age(source='name', direction='older', timestring='%Y-%m-%d', unit=unit, unit_count=unit_count)
    return index_list

  def __create_index(self, dry_run: bool, new_index: str) -> None:
    # TODO: when this may fail? -- a) index already exist b) connection error c) authentication error d) ...
    if dry_run:
      print(f"creating new index: {new_index}, DRY-RUN mode")
      CreateIndex(self.__es, new_index).do_dry_run()
    else:
      print(f"creating new index: {new_index}")
      CreateIndex(self.__es, new_index).do_action()

  def reindex_single(self, index_list: IndexList, new_index: str, old_index: str, dry_run: bool) -> None:
    # TODO: when this may fail? -- a) dest index already exist b) source index does not exist c) connection error d) authentication error e) ...
    request_body = {
      "source": {
        "index": old_index
      },
      "dest": {
        "index": new_index
      }
    }
    logger.info(msg=f"sending reindex request: {request_body}")
    if dry_run:
      logger.info(msg=f"reindexing index: {old_index} to {new_index}, mode: DRY-RUN")
      Reindex(index_list, request_body, refresh=True, wait_for_completion=True).do_dry_run()
    else:
      logger.info(msg=f"reindexing index: {old_index} to {new_index}")
      Reindex(index_list, request_body, refresh=True, wait_for_completion=True).do_action()
