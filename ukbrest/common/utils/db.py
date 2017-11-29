from sqlalchemy import create_engine


def create_table(table_name, columns, db_engine, constraints=None, drop_if_exists=True):
    with db_engine.connect() as conn:
        sql_st = """
            {drop_st}
            CREATE TABLE {create_if_not_exists} {table_name}
            (
                {columns}
                {constraints}
            )
            WITH (
                OIDS = FALSE
            );
        """.format(
            drop_st='DROP TABLE IF EXISTS {0};'.format(table_name) if drop_if_exists else '',
            create_if_not_exists='if not exists' if not drop_if_exists else '',
            table_name=table_name,
            columns=',\n'.join(columns),
            # FIXME support for more than one constraint
            constraints=',CONSTRAINT {}'.format(constraints[0]) if constraints is not None else ''
        )

        conn.execute(sql_st)


def create_indexes(table_name, columns, db_engine):
    with db_engine.connect() as conn:
        for column in columns:

            if not isinstance(column, (tuple, list)):
                column = (column,)

            index_name_suffix = '_'.join(column)
            column_name = ', '.join(column)

            index_sql = """
                CREATE INDEX ix_{table_name}_{index_name_suffix}
                ON {table_name} USING btree
                ({column_name})
            """.format(table_name=table_name, index_name_suffix=index_name_suffix, column_name=column_name)

            conn.execute(index_sql)


class DBAccess():
    def __init__(self, db_uri):
        self.db_uri = db_uri
        self.db_engine = None

    def _close_db_engine(self):
        if self.db_engine is not None:
            self.db_engine.dispose()
            del(self.db_engine)
            self.db_engine = None

    def _get_db_engine(self):
        if self.db_engine is None:
            kargs = {'pool_size': 10}
            self.db_engine = create_engine(self.db_uri, **kargs)

        return self.db_engine

    def _vacuum(self, table_name):
        with self._get_db_engine().connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
            conn.execute("""
                vacuum analyze {table_name}
            """.format(table_name=table_name))
