from sqlalchemy import create_engine
from sqlalchemy import inspect
import pandas as pd
from urllib.parse import urlparse

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
        for column_spec in columns:

            if not isinstance(column_spec, (tuple, list)):
                column_spec = (column_spec,)

            index_name_suffix = '_'.join(column_spec)
            columns_name = ', '.join(column_spec)

            index_sql = """
                CREATE INDEX ix_{table_name}_{index_name_suffix}
                ON {table_name} USING btree
                ({columns_name})
            """.format(table_name=table_name, index_name_suffix=index_name_suffix, columns_name=columns_name)

            conn.execute(index_sql)


class DBAccess():
    def __init__(self, db_uri):
        self.db_uri = db_uri
        self.db_engine = None
        parse_result = urlparse(self.db_uri)
        self.db_type = parse_result.scheme
        if self.db_type == 'sqlite':
            self.db_file = self.db_uri.split(':///')[-1]
        elif self.db_type == 'postgresql':
            self.db_host = parse_result.hostname
            self.db_port = parse_result.port
            self.db_name = parse_result.path.split('/')[-1]
            self.db_user = parse_result.username
            self.db_pass = parse_result.password

        self._fields_dtypes = {}

    def _close_db_engine(self):
        if self.db_engine is not None:
            self.db_engine.dispose()
            del(self.db_engine)
            self.db_engine = None

    def _get_db_engine(self):
        if self.db_engine is None:
            if self.db_uri is None or self.db_uri == "":
                raise ValueError('DB URI was not set')

            kargs = {'pool_size': 10}
            self.db_engine = create_engine(self.db_uri, **kargs)

        return self.db_engine

    def _vacuum(self, table_name):
        with self._get_db_engine().connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
            conn.execute("""
                vacuum analyze {table_name}
            """.format(table_name=table_name))

    def _get_table_names(self):
        return self._get_db_engine().table_names()

    def _get_column_names(self, table):
        inspector = inspect(self._get_db_engine())
        return {i['name'] for i in inspector.get_columns(table)}

    def _create_joins(self, tables, join_type='inner join'):
        if len(tables) == 0:
            return ""

        if len(tables) == 1:
            return tables[0]

        return tables[0] + ' ' + ' '.join([
            '{join_type} {table} using (eid) '.format(join_type=join_type, table=t) for t in tables[1:]
        ])

    def get_field_dtype(self, field=None):
        """Returns the type of the field. If field is None, then it just loads all fields types"""

        if field in self._fields_dtypes:
            return self._fields_dtypes[field]

        # initialize dbtypes for all fields
        field_type = pd.read_sql(
            'select distinct column_name, type '
            'from fields',
        self._get_db_engine())

        for row in field_type.itertuples():
            self._fields_dtypes[row.column_name] = row.type

        return self._fields_dtypes[field] if field in self._fields_dtypes else None
