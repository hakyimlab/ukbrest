from os.path import join, basename, splitext
from glob import glob
import re

import pandas as pd

from ukbrest.common.utils.constants import WITHDRAWALS_TABLE
from ukbrest.common.utils.db import create_table, create_indexes, DBAccess
from ukbrest.config import logger


class Postloader(DBAccess):
    def __init__(self, db_uri):
        super(Postloader, self).__init__(db_uri)

        self.patterns = {
            'points': re.compile('[\.]{1,}')
        }

    def load_withdrawals(self, withdrawals_dir):
        db_engine = self._get_db_engine()

        # create table (if not exists)
        with db_engine.connect() as conn:
            logger.info('Creating withdrawals table')
            conn.execute(f"""
                CREATE TABLE IF NOT EXISTS {WITHDRAWALS_TABLE} (
                    eid bigint primary key
                )
            """)

            for input_file in glob(join(withdrawals_dir, '*.csv')):
                logger.info(f'Reading input file {input_file}')

                data = pd.read_csv(input_file, header=None)
                data = data.rename(columns={0: 'eid'})

                n_data_before = data.shape[0]
                data = data.drop_duplicates()
                if n_data_before != data.shape[0]:
                    logger.warning(f'Duplicate IDs in file were removed ({n_data_before} vs {data.shape[0]})')

                # remove duplicates already in DB
                current_eids = pd.read_sql(f'select eid from {WITHDRAWALS_TABLE}', conn)['eid']
                data = data.loc[~data['eid'].isin(current_eids)]

                logger.info(f'Writing to SQL table: {data.shape[0]} new sample IDs')
                data.to_sql(WITHDRAWALS_TABLE, db_engine, index=False, if_exists='append')

    def load_codings(self, codings_dir):
        logger.info('Loading codings from {}'.format(codings_dir))
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
            afile_base = basename(afile)

            logger.info('Processing coding file: {}'.format(afile_base))

            data = pd.read_table(afile, sep='\t+', na_filter=False, engine='python')

            data_coding = int(splitext(afile_base)[0].split('_')[1])
            data['data_coding'] = data_coding

            data.to_sql('codings', db_engine, if_exists='append', index=False)

        create_indexes('codings', ['data_coding', 'coding', 'node_id', 'parent_id', 'selectable'], db_engine=db_engine)

        self._vacuum('codings')

    def _rename_column(self, column_name, identifier_columns):
        # first, substitute not-permitted characters
        standard_rename = re.sub(self.patterns['points'], '_', column_name.lower()).strip('_')

        if column_name in identifier_columns:
            return standard_rename

        # then apply general column format (not for identifier columns)
        return 'c{}_0_0'.format(standard_rename)

    def _get_column_type(self, pandas_type):
        if pandas_type == str:
            return 'Text'
        elif pandas_type == int:
            return 'Integer'
        elif pandas_type == float:
            return 'Continuous'
        else:
            return 'Text'

    def load_samples_data(self, data_dir, identifier_columns={}, skip_columns={}, separators={}):
        db_engine = self._get_db_engine()

        for afile in glob(join(data_dir, '*.txt')):
            filename = basename(afile)
            logger.info('Loading samples data from file: {}'.format(filename))

            sep = separators[filename] if filename in separators else ' '

            data = pd.read_table(afile, sep=sep)

            if filename in skip_columns:
                logger.info('Dropping columns: {}'.format(','.join(skip_columns[filename])))
                data = data.drop(skip_columns[filename], axis=1)

            eid_columns = identifier_columns[filename] if filename in identifier_columns else 'eid'
            if not isinstance(eid_columns, (list, tuple)):
                eid_columns = [eid_columns]

            if any(id_col not in data.columns for id_col in eid_columns):
                logger.error("File '{0}' has no identifier column ({1})".format(filename, eid_columns))
                continue

            table_name = splitext(filename)[0]

            # rename columns
            columns_rename = {old_col: self._rename_column(old_col, eid_columns) for old_col in data.columns}

            if len(eid_columns) == 1:
                columns_rename[eid_columns[0]] = 'eid'
                eid_columns[0] = 'eid'

            data = data.rename(columns=columns_rename)

            data.to_sql(table_name, db_engine, if_exists='replace', index=False)

            # add primary key
            logger.info('Adding primary key')
            with db_engine.connect() as conn:
                conn.execute("""
                    ALTER TABLE {table_name} ADD CONSTRAINT pk_{table_name} PRIMARY KEY ({id_cols});
                """.format(table_name=table_name, id_cols=','.join(eid_columns)))

            # insert new data columns into fields table
            logger.info("Adding columns to 'fields' table")
            columns_to_fields = [x for x in data.columns if x != 'eid']
            columns_dtypes_to_fields = [self._get_column_type(x) for ix, x in enumerate(data.dtypes) if data.columns[ix] != 'eid']

            fields_table_data = pd.DataFrame({
                'column_name': columns_to_fields,
                'field_id': columns_to_fields,
                'table_name': table_name,
                'type': columns_dtypes_to_fields,
            })

            fields_table_data.to_sql('fields', db_engine, index=False, if_exists='append')
