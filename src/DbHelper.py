from datetime import datetime, timezone, timedelta
from time import sleep

from psycopg2 import errors as pg_errors

from src.PostresqlWorker import PGWorker
from rules import rules


class DbHelper:
    _db = None
    _tables = {
        'uncompleted_tasks': 'uncompleted_tasks',
        'last_date': 'last_date'
    }
    _sql_create_table = {
        #'completed_tasks': 'CREATE TABLE completed_tasks(pod_name CHAR(50) NOT NULL, pod_hash CHAR(16), rule_type CHAR(50) NOT NULL, event_id CHAR(16), object_type CHAR(1), timestamp timestamptz, time_delta bigint, PRIMARY KEY (pod_name, pod_hash, rule_type, event_id));',
        'uncompleted_tasks': 'CREATE TABLE uncompleted_tasks(pod_name CHAR(50) NOT NULL, pod_hash CHAR(16), rule_type CHAR(50) NOT NULL, rule_id int NOT NULL, event_id CHAR(16), object_type CHAR(1), timestamp timestamptz, end_timestamp timestamptz);',
        'last_date': f"CREATE TABLE last_date(last_date timestamptz);", # INSERT INTO last_date (last_date) VALUES('{datetime(2020, 4, 18, tzinfo=timezone.utc)}');"
    }
    _sql_create_completed_table_templates = {
        'api': 'CREATE TABLE \"<tableName>\"(event_id CHAR(16), object_type CHAR(1), timestamp timestamptz, duration bigint, PRIMARY KEY (event_id, object_type));',
        'unit': 'CREATE TABLE \"<tableName>\"(pod_hash CHAR(16), timestamp timestamptz, duration bigint, PRIMARY KEY (pod_hash));',
    }
    _sql_insert_completed_table_columns = {
        'api': ['event_id', 'object_type', 'timestamp'],
        'unit': ['pod_hash', 'timestamp'],
    }
    _tables_keys = {
        'uncompleted_tasks': ['pod_name', 'pod_hash', 'rule_type', 'rule_id', 'event_id', 'object_type', 'timestamp', 'end_timestamp'],
        'completed_tasks_api': ['event_id', 'object_type', 'timestamp', 'duration'],
        'completed_tasks_unit': ['pod_hash', 'timestamp', 'duration'],
    }

    def __init__(self, _db_conf, _logger):
        self._db_conf = _db_conf
        self._db = PGWorker(**self._db_conf)
        self._logger = _logger

    @staticmethod
    def get_table_name(pod_name, rule_type, rule_id):
        rule_list = rules.get(pod_name, {}).get(rule_type, {})
        if len(rule_list) > rule_id:
            return f'{pod_name}_{rule_type}_{rule_id}'
        else:
            return None

    def get_tables_keys(self, table=None):
        if table is None:
            return self._tables_keys
        else:
            return self._tables_keys.get(table, None)

    def _check_table(self, table, recreate=False):
        sql = f"SELECT count(*) FROM \"{table}\" LIMIT 1;"
        try:
            if self._db.select(sql, one_val=True) is not None:
                return True
        except pg_errors.UndefinedTable:
            if recreate is False:
                self._logger.error(f'SQL failed: {sql}')
                msg = f'Table {table} not found'
                raise ValueError(msg)
            else:
                self._logger.info(f'Create table {table}')
                self._db.insert(self._sql_create_table.get(table))
                return True

    def test_db_data(self, recreate=False):
        for _, table_name in self._tables.items():
            self._check_table(table_name, recreate)

    def get_event_id_column_id(self, table):
        for idx, item in enumerate(self._tables_keys[table]):
            if 'event_id' == item:
                return idx

    @staticmethod
    def _fix_output_event_id(*list_obj: list, event_id_column_id: int):
        for i in range(len(list_obj)):
            if list_obj[i][event_id_column_id] == '':
                list_obj[i][event_id_column_id] = None

    # @fixMe: hardcode to solve some strange "select with spaces" issue
    def _fix_spaced_data(self, data: list):
        fix_col_ids = list()
        col = self.get_tables_keys('uncompleted_tasks')
        col = ['pod_name', 'pod_hash', 'rule_type', 'rule_id', 'event_id', 'object_type', 'timestamp', 'end_timestamp']
        for idx in range(len(col)):
            if col[idx] in ['pod_name', 'pod_hash', 'rule_type']:
                fix_col_ids.append(idx)

        for idx in range(len(data)):
            data_item = list(data[idx])
            for col_id in fix_col_ids:
                data_item[col_id] = data_item[col_id].strip()
            data[idx] = data_item
        return data

    def get_uncompleted_tasks(self):
        sql = "SELECT {} FROM {};".format(
            ', '.join(self._tables_keys.get('uncompleted_tasks')), self._tables.get('uncompleted_tasks'))
        unc_tasks = self._db.select(sql)
        unc_tasks = self._fix_spaced_data(unc_tasks)
        #self._fix_output_event_id(unc_tasks, self.get_event_id_column_id('uncompleted_tasks'))
        #self._logger.info(f'UNC_DATA type={type(unc_tasks)}, data={str(unc_tasks)}')
        return unc_tasks

    def get_search_date(self):
        sql = f"SELECT last_date FROM {self._tables.get('last_date')} LIMIT 1"
        timestamp = self._db.select(sql, one_val=True)
        if timestamp is None:
            today = datetime.today()
            timestamp = datetime(year=today.year, month=today.month, day=today.day, hour=0, minute=0, second=0,
                                 tzinfo=timezone.utc)
        return timestamp

    @staticmethod
    def _gen_values_string(item: dict, tasks_col):
        values = ''
        for key in tasks_col:
            if item.get(key, None) is None:
                if key in ['event_id', 'pod_hash']:
                    values += "'', "
                else:
                    values += 'Null, '
            else:
                values += f"'{item.get(key)}', "
        return values[:-2]

    '''
    def _gen_values_string_tuple(self, item: tuple, tasks_col):
        values = ''
        for idx, key in enumerate(tasks_col):
            try:
                if item[idx] is None:
                    values += 'Null, '
                else:
                    values += f"'{item[idx]}', "
            except KeyError:
                self._logger.error(f'KeyError:_gen_values_string_tuple, {type(item)} item={item}')
        return values[:-2]
    '''

    def get_table_key(self, table_name):
        for key in self._sql_create_completed_table_templates.keys():
            if key in table_name:
                return key
        return None

    def is_table(self, table_name: str, create=False):
        status = None
        try:
            return self._check_table(table_name)
        except ValueError:
            if create is False:
                return status
            else:
                table_key = self.get_table_key(table_name)
                sql = self._sql_create_completed_table_templates.get(table_key).replace('<tableName>', table_name)
                self._db.insert(sql)
                sleep(0.1)
                self._db.close_db()
                return True

    @staticmethod
    def _generate_insert_items_list(dict_obj: dict, columns: list = None):
        items_list = list()
        if columns is None:
            columns = dict_obj.keys()
        for col_name in columns:
            val = dict_obj.get(col_name, None)
            if col_name in ['timestamp', 'end_timestamp'] and val == '':
                val = None
            if val is None:
                items_list.append(f"Null")
            else:
                # @fixMe: hardcode to solve some strange "insert with spaces" issue:
                if col_name in ['pod_name', 'pod_hash', 'rule_type']:
                    val = val.strip()
                items_list.append(f"'{val}'")
        return items_list

    @staticmethod
    def time_delta(start_timestamp: datetime, end_timestamp: datetime, return_int=False):
        delta = (end_timestamp - start_timestamp).total_seconds()
        if return_int is False:
            return delta
        else:
            return int(round(delta, 0))

    def _insert_item_and_duration(self, table_name, item):
        table_key = self.get_table_key(table_name)

        columns_list = self._sql_insert_completed_table_columns.get(table_key, None).copy()
        items_list = self._generate_insert_items_list(item, columns_list)
        columns_list.append('duration')
        item.setdefault('duration', self.time_delta(item.get('timestamp'), item.get('end_timestamp'), return_int=True))
        items_list = self.dict_to_sql_insert(item, columns_list)
        sql = f"INSERT INTO \"{table_name}\" ({', '.join(columns_list)}) VALUES ({items_list});"
        self._logger.info(f'sql={sql}')
        try:
            self._db.insert(sql)
        except Exception as e:  # pg_errors.NoData
            self._logger.warning(f'Item {str(item)} already exist in table {table_name}: exception:{str(e)}')

    def dict_to_sql_insert(self, dict_obj: dict, columns: list=None):
        line = ''
        if len(dict_obj) == 0:
            self._logger.exception(f'[func: dict_to_sql_where] Empty dict {dict_obj}')

        if columns is None:
            columns = dict_obj.keys()
        for col_val in columns:
            item = dict_obj.get(col_val, None)
            if item is None:
                line += f'Null, '
            else:
                if type(item) in [str, datetime]:
                    line += f"'{item}', "
                elif type(item) == int:
                    line += f'{item}, '
                else:
                    self._logger.exception(
                        f'[func: dict_to_sql_where] val type {type(item)} is not supported, val={item}')
        return line[:-2]

    def dict_to_sql_where(self, dict_obj: dict):
        line = ''
        clue_str = ' AND '
        if len(dict_obj) == 0:
            self._logger.exception(f'[func: dict_to_sql_where] Empty dict {dict_obj}')

        for key, val in dict_obj.items():
            if key in ['timestamp', 'end_timestamp'] and val == '':
                val = None
            if val is None:
                line += f'{key}=Null{clue_str}'
            elif type(key) in [str, datetime]:
                line += f"{key}='{val}'{clue_str}"
            elif type(key) == int:
                line += f'{key}={val}{clue_str}'
            else:
                self._logger.exception(
                    f'[func: dict_to_sql_where] val type {type(key)} is not supported, val={key}')
        return line[:-len(clue_str)]

    def _is_item_exist(self, table_name: str, item: dict):
        sql = f"SELECT count(*) FROM \"{table_name}\" WHERE {self.dict_to_sql_where(item)}"
        self._logger.info(f'sql={sql}')
        if self._db.select(sql, one_val=True) == 0:
            return False
        else:
            return True

    def _insert_item(self, table_name: str, item: dict, columns: list=None):
        if columns is None:
            columns = item.keys()

        items_list = self._generate_insert_items_list(item, columns)
        sql = f"INSERT INTO \"{table_name}\" ({', '.join(columns)}) VALUES ({', '.join(items_list)});"
        self._logger.info(f'sql={sql}')
        self._db.insert(sql)

    def _sync_uncompleted(self, data: list):
        columns = self._tables_keys.get('uncompleted_tasks')
        db_items = list()
        for data_item in data:
            db_item = dict()
            for col_name in columns:
                db_item.setdefault(col_name, data_item.get(col_name, ''))
            db_items.append(db_item)

        table_name = self._tables.get('uncompleted_tasks', None)
        for item in db_items:
            if item.get('timestamp', None) in ['', None] and item.get('end_timestamp', '') in ['', None]:
                self._logger.warning(f'Bad data item: item={item}')
            else:
                if not self._is_item_exist(table_name, item):
                    self._insert_item(table_name, item)

    def _update_last_date(self, last_scan_date: datetime, error: bool):
        if error:
            sql = f"UPDATE {self._tables.get('last_date')} SET last_date='{last_scan_date}';"
        else:
            if datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0,
                                         tzinfo=timezone.utc) > last_scan_date:
                db_date = last_scan_date + timedelta(days=1)
                db_date = db_date.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=timezone.utc)
            else:
                db_date = last_scan_date
            sql = f"UPDATE {self._tables.get('last_date')} SET last_date='{db_date}';"
        self._db.update(sql)

    def save_state(self, found, uncompleted, last_scan_date, error):

        # create all needed tables
        table_list = list()
        for item in found:
            table_name = self.get_table_name(item.get('pod_name'), item.get('rule_type'), item.get('rule_id'))
            if table_name not in table_list:
                self.is_table(table_name, create=True)
                table_list.append(table_name)

        # write pair tasks
        self._db.set_keep_connection_mode(True)
        for item in found:
            table_name = self.get_table_name(item.get('pod_name'), item.get('rule_type'), item.get('rule_id'))
            self._insert_item_and_duration(table_name, item)
        self._db.commit(force=True)

        # sync uncompleted tasks
        self._sync_uncompleted(uncompleted)
        self._db.commit(force=True)

        self._db.set_keep_connection_mode(False)
        self._update_last_date(last_scan_date, error)
        self._db.close_db()
        '''
        # переподключаемся в режим одного подключения к базе
        self._db.set_keep_connection_mode(True)

        # пишем данные о парных тасках
        sql_template = f"INSERT INTO {self._tables.get('completed_tasks')} ({', '.join(tasks_col)}) VALUES (<values>);"
        for item in tasks_data:
            sql = sql_template.replace('<values>', self._gen_values_string(item, tasks_col))
            self._db.insert(sql)
        self._db.commit()

        # обновляем незавершенные таски
        # генерируем список id из uncompleted_tasks
        current_unc_ids = dict()
        for idx, item in enumerate(uncompleted_tasks):
            if item.get('event_id', None) is not None:
                current_unc_ids.setdefault(f"id_{item.get('event_id')}", idx)
            elif item.get('pod_hash', None) is not None:
                current_unc_ids.setdefault(f"hash_{item.get('pod_hash')}", idx)

        sql = f"SELECT event_id, pod_hash FROM {self._tables.get('uncompleted_tasks')}"
        # забираем из базы список event_id и pod_hash незаконченных тасков и их id
        db_data = self._db.select(sql)
        # проходим по бд списку
        for db_line in db_data:
            # если элемент из БД есть в нашем списке, то удаляем из нашего списка
            if db_line[1] != '' and f"id_{db_line[1]}" in current_unc_ids.keys():
                del uncompleted_tasks[current_unc_ids.get(f"id_{db_line[1]}")]
            elif db_line[2] != '' and f"hash_{db_line[1]}" in current_unc_ids.keys():
                del uncompleted_tasks[current_unc_ids.get(f"hash_{db_line[2]}")]
            # если элемента нет в нашем списке, то удаляем строчку из БД
            else:
                sql = f"DELETE FROM {self._tables.get('uncompleted_tasks')} WHERE id={db_line[0]}"
                self._db.update(sql)
        self._db.commit()

        # проходим по нашему оставшемуся списку
        sql_template = f"INSERT INTO {self._tables.get('uncompleted_tasks')} ({', '.join(self.get_tables_keys('uncompleted_tasks'))}) VALUES (<values>);"
        for item in uncompleted_tasks:
            # добавляем в базу записи
            sql = sql_template.replace('<values>', self._gen_values_string(item, self.get_tables_keys('uncompleted_tasks')))
            self._db.insert(sql)
        self._db.set_keep_connection_mode(False)

        if error:
            sql = f"UPDATE {self._tables.get('last_date')} SET last_date='{last_scan_date}';"
        else:
            tomorrow = last_scan_date + timedelta(days=1)
            tomorrow = tomorrow.replace(hour=0, minute=0, second=0)
            sql = f"UPDATE {self._tables.get('last_date')} SET last_date='{tomorrow}';"
        self._db.update(sql)
        '''
