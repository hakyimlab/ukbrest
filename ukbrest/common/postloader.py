from os.path import join, basename, splitext
from glob import glob

import pandas as pd
from sqlalchemy import create_engine


class Postloader():
    def __init__(self, db_uri):
        self._db_uri = db_uri
        self.db_engine = None

    def _get_db_engine(self):
        if self.db_engine is None:
            kargs = {'pool_size': 10}
            self.db_engine = create_engine(self._db_uri, **kargs)

        return self.db_engine

    def load_codings(self, codings_dir):
        db_engine = self._get_db_engine()

        with db_engine.connect() as con:
            con.execute("""
                CREATE TABLE codings
                (
                    data_coding bigint NOT NULL,
                    coding text NOT NULL,
                    meaning text NOT NULL,
                    node_id bigint NULL,
                    parent_id bigint NULL,
                    selectable boolean NULL,
                    CONSTRAINT pk_codings PRIMARY KEY (data_coding, coding, meaning)
                );
            """)

        for afile in glob(join(codings_dir, '*.tsv')):
            data = pd.read_table(afile)

            data_coding = int(splitext(basename(afile))[0].split('_')[1])
            data['data_coding'] = data_coding

            data.to_sql('codings', db_engine, if_exists='append', index=False)

        with db_engine.connect() as con:
            for column in ('data_coding', 'coding', 'node_id', 'parent_id', 'selectable'):
                index_sql = """
                    CREATE INDEX ix_codings_{column_name}
                    ON codings USING btree
                    ({column_name})
                """.format(column_name=column)

                con.execute(index_sql)