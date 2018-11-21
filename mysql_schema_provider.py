import pymysql
from mysql_info import db_info


class MysqlSchemaProvider:
    def __init__(self, host, user, pw):
        self.conn = pymysql.connect(host=host, user=user, passwd=pw)
        self.schema = {}

    def generate_schema(self):
        self.schema = {}
        for database in self._get_databases():
            self._use_database(database)
            for table in self._get_tables():
                for column in self._get_columns(table):
                    self.schema.setdefault(database, {}).setdefault(table, {}).setdefault(column['Field'], column)

    def _get_databases(self):
        with self.conn.cursor() as cur:
            cur.execute('show databases;')
            for row in cur.fetchall():
                if row[0] in ['information_schema', 'performance_schema']:
                    continue
                yield row[0]

    def _use_database(self, database):
        with self.conn.cursor() as cur:
            cur.execute('use {}'.format(database))

    def _get_tables(self):
        with self.conn.cursor() as cur:
            cur.execute('show tables;')
            for row in cur.fetchall():
                yield row[0]

    def _get_columns(self, table):
        with self.conn.cursor() as cur:
            cur.execute('show columns from {}'.format(table))
            for row in cur.fetchall():
                yield {
                    'Field': row[0],
                    'Type': row[1],
                    'Null': row[2],
                    'Key': row[3],
                    'Default': row[4],
                    'Extra': row[5],
                }

if __name__ == '__main__':
    p = MysqlSchemaProvider(db_info.host, db_info.user, db_info.pw)
    p.generate_schema()
    print(p.schema)