import logging as lgng

import psycopg2
# brew install postgresql (for os x)
# psycopg2==2.8.5


class PGWorker:
    __conn = None

    def __init__(self, host, port, user, password, db_name, keep_connection_mode=False):
        self.__host = host
        self.__port = str(port)
        self.__user = user
        self.__password = password
        self.__db_name = db_name
        self._keep_connection_mode = keep_connection_mode

    def __connect(self):
        self.__conn = psycopg2.connect(dbname=self.__db_name, user=self.__user, password=self.__password,
                                       host=self.__host, port=self.__port)

    def __disconnect(self):
        if self.__conn:
            self.__conn.close()
            self.__conn = None

    def __del__(self):
        try:
            self.__disconnect()
        except:
            pass

    def close_db(self):
        self.__disconnect()

    def set_keep_connection_mode(self, keep_connection_mode):
        self._keep_connection_mode = keep_connection_mode
        if self._keep_connection_mode is False:
            if self.__conn:
                self.__conn.commit()
            self.close_db()

    def commit(self, force=False):
        if self.__conn and (force or self._keep_connection_mode):
            self.__conn.commit()

    def select(self, sql, one_val=False):
        lgng.info(f'DEBUG_PG:{sql}')
        if self.__conn is None or self._keep_connection_mode is False:
            self.__connect()

        cursor = self.__conn.cursor()
        cursor.execute(sql)
        results = cursor.fetchall()

        if self._keep_connection_mode is False:
            self.__disconnect()
        if one_val:
            if len(results) > 0 and len(results[0]) > 0:
                return results[0][0]
            else:
                return None
        return results

    def update(self, sql):
        lgng.info(f'DEBUG_PG:{sql}')
        if self.__conn is None or self._keep_connection_mode is False:
            self.__connect()
        try:
            cursor = self.__conn.cursor()
            cursor.execute(sql)
            self.__conn.commit()
            if self._keep_connection_mode is False:
                self.__disconnect()
        except Exception as e:
            lgng.warning(f'Update exception: sql={sql}, exception={str(e)}')
            self.__disconnect()

    def insert(self, sql):
        lgng.info(f'DEBUG_PG:{sql}')
        if self.__conn is None or self._keep_connection_mode is False:
            self.__connect()
        try:
            cursor = self.__conn.cursor()
            cursor.execute(sql)
            # @ToDo: refactor this code to autoAdd RETURNING columnName to the end of sql, if return=columnName
            try:
                results = cursor.fetchall()
                if len(results) > 0 and len(results[0]) > 0:
                    _id = results[0][0]
            except psycopg2.ProgrammingError:
                _id = True
            if self._keep_connection_mode is False:
                self.__conn.commit()
                self.__disconnect()
            return _id
        except Exception as e:
            lgng.warning(f'Insert exception: sql={sql}, exception={str(e)}')
            self.__disconnect()
