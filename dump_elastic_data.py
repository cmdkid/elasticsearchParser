import os
from datetime import datetime
import logging
import pickle

from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search

from conf import conf
from src.sys_helper import ping


# init logger
log_level = int(os.getenv('PARSER_LOG_LEVEL', conf.get('log', None).get('level', None)))
log_format = os.getenv('PARSER_LOG_FORMAT', conf.get('log', None).get('format', None))
logging.basicConfig(level=log_level, format=log_format)
logger = logging.getLogger('es-parser')


def get_es_data(url: str, search_prefix: str, _search_date: datetime):
    cur_index = '{}{}'.format(search_prefix, _search_date.strftime("%Y.%m.%d"))
    logger.info(f'Current es_index: {cur_index}')
    es_client = Elasticsearch(url)
    s = Search(using=es_client, index=cur_index).sort({"timestamp": {"order": "asc"}})
    logger.info(f'Records found: {s.count()}')
    '''
    filter_date = _search_date.strftime("%Y-%m-%dT%H:%M:%S.%f000%z")
    logger.info(f'Current es_index: {cur_index}')
    es_client = Elasticsearch(es_url)
    s = es_client.search(
        index=cur_index,
        body={"query": {"bool": {"filter": {"range": {"@timestamp": {"gte": filter_date}}}}}}
    )
    logger.info(f'Records found: {len(s)}')
    '''
    return s


def gen_filename(prefix: str, extension: str):
    return f'{prefix}_{int(datetime.now().timestamp()*1000000)}.{extension}'


def dump_obj(obj, file_path: str):
    with open(file_path, "wb") as file:
        pickle.dump(obj, file)


if __name__ == '__main__':
    dump_folder = f'./dump_{int(datetime.now().timestamp())}'
    es_url = os.getenv('ELASTICSEARCH_URL', conf.get('elastic_server', None))
    search_date = os.getenv('DUMPER_DUMP_DATE', conf.get('dumper', {}).get('dump_date', None))

    if not ping(es_url):
        msg = f'Url {es_url} is not accessible.'
        logger.exception(msg)

    if search_date is None:
        msg = 'DUMPER_DUMP_DATE and Conf.py:dumper.dump_date was not set.'
        logger.exception(msg)

    if os.path.exists(dump_folder):
        msg = f'Path {dump_folder} already exist!'
        logger.exception(msg)

    os.makedirs(dump_folder)
    es_data = get_es_data(es_url, conf.get('es').get('index_prefix'), search_date)

    cnt = 0
    for idx, hit in enumerate(es_data.scan()):
        cnt = idx+1
        f_path = os.path.join(dump_folder, gen_filename('hit', 'dump'))
        logger.info(f'Dumping #{cnt} to {f_path}..')
        dump_obj(hit, f_path)

    logger.info(f'Dump {cnt} items complete')
