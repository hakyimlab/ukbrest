from sqlalchemy import create_engine


def create_table(table_name, columns, constraints, db_engine, drop_if_exists=True):
    with db_engine.connect() as conn:
        sql_st = """
            {drop_st}
            CREATE TABLE {table_name}
            (
                {columns},
                CONSTRAINT {constraints}
            )
            WITH (
                OIDS = FALSE
            );
        """.format(
            drop_st='DROP TABLE IF EXISTS {0};'.format(table_name) if drop_if_exists else '',
            table_name=table_name,
            columns=',\n'.join(columns),
            # FIXME support for more than one constraint
            constraints=constraints[0]
        )

        conn.execute(sql_st)


def create_indexes(table_name, columns, db_engine):
    with db_engine.connect() as conn:
        for column in columns:
            index_sql = """
                CREATE INDEX ix_{table_name}_{column_name}
                ON {table_name} USING btree
                ({column_name})
            """.format(table_name=table_name, column_name=column)

            conn.execute(index_sql)
