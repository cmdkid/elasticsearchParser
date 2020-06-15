import os
import logging as lgng
from datetime import datetime, timezone
from time import sleep
from socket import gaierror

from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search
from dateutil import parser
from urllib3.exceptions import NewConnectionError

from src.sys_helper import ping
from src.DbHelper import DbHelper
from src.HitHelper import HitHelper
from src.RulesHelper import RulesHelper
from src.TasksHelper import TasksHelper
from src.ElasticsearchMock import ElasticsearchMock, SearchMock, EmptySearchMock
from conf import conf

# init logger
log_level = int(os.getenv('PARSER_LOG_LEVEL', conf.get('log', None).get('level', None)))
log_format = os.getenv('PARSER_LOG_FORMAT', conf.get('log', None).get('format', None))
lgng.basicConfig(level=log_level, format=log_format)
logger = lgng.getLogger('es-parser')


def fix_string_bool(string: str):
    if string.lower() == 'true':
        return True
    elif string.lower() == 'false':
        return False
    else:
        return string


def get_conf_data(_conf):
    _es_url = os.getenv('ELASTICSEARCH_URL', conf.get('elastic_server', None))
    _db = conf.get('db', {})
    _dumper_conf = conf.get('dumper', {})
    if 'PSQL_HOST' in os.environ:
        _db['host'] = os.getenv('PSQL_HOST')
    if 'PSQL_PORT' in os.environ:
        _db['port'] = os.getenv('PSQL_PORT')
    if 'PSQL_USER' in os.environ:
        _db['user'] = os.getenv('PSQL_USER')
    if 'PSQL_DB' in os.environ:
        _db['db_name'] = os.getenv('PSQL_DB')
    if 'PSQL_PASSWD' in os.environ:
        _db['password'] = os.getenv('PSQL_PASSWD')
    if 'DUMP_MODE' in os.environ:
        _dumper_conf['dump_mode'] = fix_string_bool(os.getenv('DUMP_MODE'))
    if 'DUMP_DATE' in os.environ:
        _dumper_conf['dump_date'] = os.getenv('DUMP_DATE')
    if 'DUMP_USE' in os.environ:
        _dumper_conf['dump_use'] = fix_string_bool(os.getenv('DUMP_USE'))
    return _es_url, _db, _dumper_conf


def test_connection(_es_url, _db_conf):
    if not ping(_es_url):
        logger.error(f'Elastic server {_es_url} is down or not accessible from this host!', exc_info=True)

    db_url = '{}:{}'.format(_db_conf.get('host'), _db_conf.get('port'))
    if not ping(db_url):
        logger.error(f'Db {db_url} is down or not accessible from this host!', exc_info=True)


def get_es_data(search_prefix: str, _search_date: datetime):
    cur_index = '{}{}'.format(search_prefix, _search_date.strftime("%Y.%m.%d"))
    filter_date = _search_date.strftime("%Y-%m-%dT%H:%M:%S.%f000%z")
    logger.info(f'Current es_index: {cur_index}')
    try:
        es_client = Elasticsearch(es_url)
        #s = es_client.search(
        #    index=cur_index,
        #    body={"query": {"bool": {"filter": {"range": {"@timestamp": {"gte": filter_date}}}}}}
        #)
        #logger.info(f'Records found: {len(s)}')
        # .execute()
        s = Search(using=es_client, index=cur_index).sort({"timestamp": {"order": "asc"}}) #_search_date
        logger.info(f'Records found: {s.count()}')
        return s
    except (gaierror, NewConnectionError) as _e:
        logger.exception(f'Elasticsearch connection error: url={es_url}, exception={str(_e)}')


def get_es_dump_data(search_prefix: str, _search_date: str):
    dump_path = conf.get('dumper').get('dump_path')
    logger.info(f'Current es_index: {dump_path}')
    es_client = ElasticsearchMock(dump_path)
    s = SearchMock(using=es_client, index='')
    logger.info(f'Records found: {s.count()}')
    return s


if __name__ == '__main__':
    logger.info('[START PARSER WORK]')

    # Проверить доступность сервисов
    es_url, db_conf, dumper_conf = get_conf_data(conf)
    dump_mode = dumper_conf.get('dump_mode', False)
    dump_use = dumper_conf.get('dump_use', False)
    # @ToDo: uncomment when release
    if dump_use is False:
        test_connection(es_url, db_conf)
    db = DbHelper(db_conf, logger)

    # Проверить наполнение базы и  инициализировать, если необходимо
    db.test_db_data(recreate=True)

    cycle_count = 0
    while True:
        cycle_count += 1
        logger.info(f'[START PARSER CYCLE] {cycle_count}')
        error = False
        # Считать список незавершенных задач из uncompleted_tasks в словарь
        tasks = TasksHelper(uncompleted_tasks=db.get_uncompleted_tasks(),
                            uncompleted_tasks_cols=db.get_tables_keys('uncompleted_tasks'))

        # Считать последнюю дату/время лога кубера с базы. Если даты нет, то поставить сегодняшнюю, 0:00 gmt-0
        search_date = db.get_search_date()
        # search_date = datetime(year=2020, month=4, day=18, tzinfo=timezone.utc)
        logger.info(f'[SEARCH DATE: {search_date.strftime("%Y-%m-%dT%H:%M:%S.%f000%z")}]')
        last_scan_date = search_date
        # Сгенерировать поисковый запрос на эластик с за заданного дата/время, до конца дня, выравненные по возрастанию
        if dump_use is True:
            # skip es dump load, if we got task dump or db dump
            if dump_mode == 'dump_tasks':
                es_data = get_es_dump_data(conf.get('es').get('index_prefix'), search_date)
                logger.info(f'[ES DUMP LOADED]')
            else:
                es_data = EmptySearchMock()
                logger.info(f'[SKIP ES DUMP PROCESSING]')
        else:
            logger.info(f'[READ FROM ES] {cycle_count}')
            es_data = get_es_data(conf.get('es').get('index_prefix'), search_date)
        # По каждой записи
        try:
            for hit in es_data.scan():
                hit_obj = HitHelper(hit, logger)
                # Узнать под и его хеш и дату из эластика
                # Обновить дату последней записи
                last_scan_date = parser.parse(hit_obj.get_hit_data().get('@timestamp', None))
                if last_scan_date < search_date and dump_mode is False:
                    continue

                pod_name, pod_hash = hit_obj.get_pod_info()

                # Проверить, есть ли на этот под правило
                if RulesHelper.is_pod_in_rules(pod_name):
                    # Проверить, подходит ли задача по фильтрам
                    rule_name, rule_id, trigger_type = RulesHelper.is_triggered_log(pod_name, hit_obj.get_log())
                    if rule_name:
                        logger.info(f'is_triggered_log: rule_name={rule_name}, trigger_type={trigger_type}, pod_name={pod_name}, pod_hash={pod_hash}')
                        # Распарсить данные, дату лога [и тип задачи] и добавить задачу в список вставок
                        data = RulesHelper.parse_rule(rule_name, trigger_type, hit_obj)
                        data.setdefault('rule_id', rule_id)
                        logger.info(f'add_data: data={data}')
                        tasks.add_data(data)

        except Exception as e:
            logger.error(f'Hit cycle error:{str(e)}')
            error = True

        # сагрегировать собранные данные в пары, найти пары старым незавершенным, обновить список незавершенных
        if dump_mode == 'dump_tasks':
            tasks_dump_path = dumper_conf.get('tasks_dump_path', 'tasks.dump')
            tasks.dump_tasks_to_file(tasks_dump_path)
            logger.info(f'[TASKS DUMP CREATED] {tasks_dump_path}')
        else:
            if dump_use is True:
                tasks_dump_path = dumper_conf.get('tasks_dump_path', 'tasks.dump')
                tasks.load_tasks_obj(tasks_dump_path)
                logger.info(f'[TASKS DUMP LOADED] {tasks_dump_path}')

        logger.info(f'[PROCESS DATA] {cycle_count}')
        tasks.process_data()

        # сохранить в базу парные таски, незавершенные, и последнюю дату сканирования.
        # Если  не сбой (error), то поправить дату на день вперед, чтобы следующим циклом прошелся новый день
        logger.info(f'[SAVE DATA TO DB] {cycle_count}')
        db.save_state(found=tasks.get_found_tasks(), uncompleted=tasks.get_unc_tasks(), last_scan_date=last_scan_date,
                      error=error)

        if error:
            logger.info(f'[END PARSER WORK WITH ERROR] {cycle_count}')
            exit(1)
        else:
            wait_sec = 30*60
            logger.info(f'[END PARSER CYCLE] {cycle_count}')
            logger.info(f'[SLEEP] {wait_sec} sec')
            sleep(wait_sec)
