from os.path import join, basename, splitext
from glob import glob

import pandas as pd
from sqlalchemy import create_engine

from ukbrest.common.utils.db import create_table, create_indexes


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

        create_table('codings',
            columns=[
                'data_coding bigint NOT NULL',
                'coding text NOT NULL',
                'meaning text NOT NULL',
                'node_id bigint NULL',
                'parent_id bigint NULL',
                'selectable boolean NULL',
            ],
            constraints=[
                'pk_codings PRIMARY KEY (data_coding, coding, meaning)'
            ],
            db_engine=self._get_db_engine()
         )

        for afile in glob(join(codings_dir, '*.tsv')):
            data = pd.read_table(afile)

            data_coding = int(splitext(basename(afile))[0].split('_')[1])
            data['data_coding'] = data_coding

            data.to_sql('codings', db_engine, if_exists='append', index=False)

        create_indexes('codings', ['data_coding', 'coding', 'node_id', 'parent_id', 'selectable'], db_engine=db_engine)
