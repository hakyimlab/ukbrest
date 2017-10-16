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
                    field_id bigint NOT NULL,
                    coding text NULL,
                    meaning text NOT NULL
                );
            """)

        for afile in glob(join(codings_dir, '*.tsv')):
            data = pd.read_table(afile)
            field_id = int(splitext(basename(afile))[0].split('_')[1])
            data['field_id'] = field_id
            data.to_sql('codings', db_engine, if_exists='append', index=False)
