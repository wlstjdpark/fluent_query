class QueryGenerator:
    def __init__(self, database, table, limit_count=1, **column_kwargs):
        # input set
        self.limit_count = limit_count
        self.database = database
        self.table = table
        self.column_dic = column_kwargs

    def _generate_select(self):
        return 'select * from {}.{}'.format(self.database, self.table)

    def _generate_condition(self):
        if len(self.column_dic) == 0:
            return None

        condition_list = []
        for column, value in self.column_dic.items():
            condition_list.append("{}='{}'".format(column, value))
        return ' and '.join(condition_list)

    def _generate_limit(self):
        return 'limit {}'.format(self.limit_count)

    def generate_query(self):
        select = self._generate_select()
        condition = self._generate_condition()
        limit = self._generate_limit()

        if condition is None:
            result_query = '{} {};'.format(select, limit)
        else:
            result_query = '{} where {} {};'.format(select, condition, limit)

        return result_query

if __name__ == '__main__':
    g = QueryGenerator('richgo', 'joinsland01', 1, STAT_NAME='1.1.주요 통화금융지표', STAT_CODE='010Y002')
    q = g.generate_query()
    print(q)

    from mysql_info import db_info
    import pymysql
    conn = pymysql.connect(host=db_info.host, user=db_info.user, passwd=db_info.pw)
    conn.set_charset('utf8')
    with conn.cursor() as cur:
        try:
            cur.execute(q)
            desc = cur.description
            result = [dict(zip([col[0] for col in desc], row))
            for row in cur.fetchall()]
        except Exception as e:
            result = str(e)
        finally:
            conn.close()
    print(result)
