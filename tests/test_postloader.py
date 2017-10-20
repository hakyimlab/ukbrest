import os
import numpy as np
import pandas as pd
from sqlalchemy import create_engine

from ukbrest.common.pheno2sql import Pheno2SQL
from ukbrest.common.postloader import Postloader
from tests.settings import POSTGRESQL_ENGINE
from tests.utils import get_repository_path, DBTest


class PostloaderTest(DBTest):
    def test_postload_codings_table_basic(self):
        # prepare
        directory = get_repository_path('postloader/codings01')

        # run
        pl = Postloader(POSTGRESQL_ENGINE)
        pl.load_codings(directory)

        # validate
        ## Check samples table exists
        table = pd.read_sql("""
            SELECT EXISTS (
                SELECT 1 FROM pg_tables
                WHERE schemaname = 'public' AND tablename = '{}'
            )""".format('codings'),
            create_engine(POSTGRESQL_ENGINE))

        assert table.iloc[0, 0]

        codings = pd.read_sql("select * from codings order by data_coding, coding", create_engine(POSTGRESQL_ENGINE))
        assert codings is not None
        expected_columns = ['data_coding', 'coding', 'meaning']
        assert len(codings.columns) >= len(expected_columns)
        assert all(x in codings.columns for x in expected_columns)

        assert not codings.empty
        assert codings.shape[0] == 4

        cidx = 0
        assert codings.loc[cidx, 'data_coding'] == 7
        assert codings.loc[cidx, 'coding'] == '0'
        assert codings.loc[cidx, 'meaning'] == 'No'

        cidx += 1
        assert codings.loc[cidx, 'data_coding'] == 7
        assert codings.loc[cidx, 'coding'] == '1'
        assert codings.loc[cidx, 'meaning'] == 'Yes'

        cidx += 1
        assert codings.loc[cidx, 'data_coding'] == 9
        assert codings.loc[cidx, 'coding'] == '0'
        assert codings.loc[cidx, 'meaning'] == 'Female'

        cidx += 1
        assert codings.loc[cidx, 'data_coding'] == 9
        assert codings.loc[cidx, 'coding'] == '1'
        assert codings.loc[cidx, 'meaning'] == 'Male'

    def test_postload_codings_negative_coding(self):
        # prepare
        directory = get_repository_path('postloader/codings02_negative')

        # run
        pl = Postloader(POSTGRESQL_ENGINE)
        pl.load_codings(directory)

        # validate
        ## Check samples table exists
        table = pd.read_sql("""
            SELECT EXISTS (
                SELECT 1 FROM pg_tables
                WHERE schemaname = 'public' AND tablename = '{}'
            )""".format('codings'),
            create_engine(POSTGRESQL_ENGINE))

        assert table.iloc[0, 0]

        codings = pd.read_sql("select * from codings order by data_coding, coding", create_engine(POSTGRESQL_ENGINE))
        assert codings is not None
        expected_columns = ['data_coding', 'coding', 'meaning']
        assert len(codings.columns) >= len(expected_columns)
        assert all(x in codings.columns for x in expected_columns)

        assert not codings.empty
        assert codings.shape[0] == 2

        cidx = 0
        assert codings.loc[cidx, 'data_coding'] == 13
        assert codings.loc[cidx, 'coding'] == '-1'
        assert codings.loc[cidx, 'meaning'] == 'Date uncertain or unknown'

        cidx += 1
        assert codings.loc[cidx, 'data_coding'] == 13
        assert codings.loc[cidx, 'coding'] == '-3'
        assert codings.loc[cidx, 'meaning'] == 'Preferred not to answer'

    def test_postload_codings_tree_structured(self):
        # prepare
        directory = get_repository_path('postloader/codings03_tree')

        # run
        pl = Postloader(POSTGRESQL_ENGINE)
        pl.load_codings(directory)

        # validate
        ## Check samples table exists
        table = pd.read_sql("""
            SELECT EXISTS (
                SELECT 1 FROM pg_tables
                WHERE schemaname = 'public' AND tablename = '{}'
            )""".format('codings'),
            create_engine(POSTGRESQL_ENGINE))

        assert table.iloc[0, 0]

        codings = pd.read_sql("select * from codings order by data_coding, coding::int, node_id asc", create_engine(POSTGRESQL_ENGINE))
        assert codings is not None
        expected_columns = ['data_coding', 'coding', 'meaning', 'node_id', 'parent_id', 'selectable']
        assert len(codings.columns) >= len(expected_columns)
        assert all(x in codings.columns for x in expected_columns)

        assert not codings.empty
        assert codings.shape[0] == 474 + 2

        assert set(np.unique(codings.loc[:, 'data_coding'])) == {6, 7}

        cidx = 0
        assert codings.loc[cidx, 'data_coding'] == 6
        assert codings.loc[cidx, 'coding'] == '-1'
        assert codings.loc[cidx, 'meaning'] == 'cardiovascular'
        assert codings.loc[cidx, 'node_id'] == 1071
        assert codings.loc[cidx, 'parent_id'] == 0
        assert codings.loc[cidx, 'selectable'] == False

        cidx += 1
        assert codings.loc[cidx, 'data_coding'] == 6
        assert codings.loc[cidx, 'coding'] == '-1'
        assert codings.loc[cidx, 'meaning'] == 'respiratory/ent'
        assert codings.loc[cidx, 'node_id'] == 1072
        assert codings.loc[cidx, 'parent_id'] == 0
        assert codings.loc[cidx, 'selectable'] == False

        cidx = 10
        assert codings.loc[cidx, 'data_coding'] == 6
        assert codings.loc[cidx, 'coding'] == '-1'
        assert codings.loc[cidx, 'meaning'] == 'cerebrovascular disease'
        assert codings.loc[cidx, 'node_id'] == 1083
        assert codings.loc[cidx, 'parent_id'] == 1071
        assert codings.loc[cidx, 'selectable'] == False

        cidx = 28
        assert codings.loc[cidx, 'data_coding'] == 6
        assert codings.loc[cidx, 'coding'] == '1065'
        assert codings.loc[cidx, 'meaning'] == 'hypertension'
        assert codings.loc[cidx, 'node_id'] == 1081
        assert codings.loc[cidx, 'parent_id'] == 1071
        assert codings.loc[cidx, 'selectable'] == True

        cidx = 277
        assert codings.loc[cidx, 'data_coding'] == 6
        assert codings.loc[cidx, 'coding'] == '1478'
        assert codings.loc[cidx, 'meaning'] == 'cervical spondylosis'
        assert codings.loc[cidx, 'node_id'] == 1541
        assert codings.loc[cidx, 'parent_id'] == 1608
        assert codings.loc[cidx, 'selectable'] == True

        cidx = 473
        assert codings.loc[cidx, 'data_coding'] == 6
        assert codings.loc[cidx, 'coding'] == '99999'
        assert codings.loc[cidx, 'meaning'] == 'unclassifiable'
        assert codings.loc[cidx, 'node_id'] == 99999
        assert codings.loc[cidx, 'parent_id'] == 0
        assert codings.loc[cidx, 'selectable'] == False

        cidx = 474
        assert codings.loc[cidx, 'data_coding'] == 7
        assert codings.loc[cidx, 'coding'] == '0'
        assert codings.loc[cidx, 'meaning'] == 'No'
        assert pd.isnull(codings.loc[cidx, 'node_id'])
        assert pd.isnull(codings.loc[cidx, 'parent_id'])
        assert pd.isnull(codings.loc[cidx, 'selectable'])

        cidx = 475
        assert codings.loc[cidx, 'data_coding'] == 7
        assert codings.loc[cidx, 'coding'] == '1'
        assert codings.loc[cidx, 'meaning'] == 'Yes'
        assert pd.isnull(codings.loc[cidx, 'node_id'])
        assert pd.isnull(codings.loc[cidx, 'parent_id'])
        assert pd.isnull(codings.loc[cidx, 'selectable'])

    def test_postload_codings_check_constrains_exist(self):
        # prepare
        directory = get_repository_path('postloader/codings03_tree')

        # run
        pl = Postloader(POSTGRESQL_ENGINE)
        pl.load_codings(directory)

        # Validate
        ## Check samples table exists
        table = pd.read_sql("""
            SELECT EXISTS (
                SELECT 1 FROM pg_tables
                WHERE schemaname = 'public' AND tablename = '{}'
            )""".format('codings'),
            create_engine(POSTGRESQL_ENGINE))

        assert table.iloc[0, 0]

        # primary key
        constraint_sql = self._get_table_contrains('codings', relationship_query='pk_%%')
        constraints_results = pd.read_sql(constraint_sql, create_engine(POSTGRESQL_ENGINE))
        assert constraints_results is not None
        assert not constraints_results.empty
        columns = constraints_results['column_name'].tolist()
        assert len(columns) == 3
        assert 'data_coding' in columns
        assert 'coding' in columns
        assert 'meaning' in columns

        # index on 'event' column
        constraint_sql = self._get_table_contrains('codings', relationship_query='ix_%%')
        constraints_results = pd.read_sql(constraint_sql, create_engine(POSTGRESQL_ENGINE))
        assert constraints_results is not None
        assert not constraints_results.empty
        columns = constraints_results['column_name'].tolist()
        assert len(columns) == 5
        assert 'data_coding' in columns
        assert 'coding' in columns
        assert 'node_id' in columns
        assert 'parent_id' in columns
        assert 'selectable' in columns

    def test_postload_codings_vacuum(self):
        # prepare
        directory = get_repository_path('postloader/codings03_tree')

        # run
        pl = Postloader(POSTGRESQL_ENGINE)
        pl.load_codings(directory)

        # Validate
        db_engine = create_engine(POSTGRESQL_ENGINE)

        ## Check samples table exists
        table = pd.read_sql("""
            SELECT EXISTS (
                SELECT 1 FROM pg_tables
                WHERE schemaname = 'public' AND tablename = '{}'
            )""".format('codings'), db_engine)

        assert table.iloc[0, 0]

        vacuum_data = pd.DataFrame()
        query_count = 0

        # FIXME waits for vacuum to finish
        while vacuum_data.empty and query_count < 150:
            vacuum_data = pd.read_sql("""
                select relname, last_vacuum, last_analyze
                from pg_stat_user_tables
                where schemaname = 'public' and last_vacuum is not null and last_analyze is not null
            """, db_engine)
            query_count += 1

        assert vacuum_data is not None
        assert not vacuum_data.empty
        tables = vacuum_data['relname'].tolist()
        assert 'codings' in tables

    def test_postload_load_samples_data_one_file(self):
        # prepare
        directory = get_repository_path('postloader/samples_data01')

        # run
        pl = Postloader(POSTGRESQL_ENGINE)
        pl.load_samples_data(directory)

        # Validate
        db_engine = create_engine(POSTGRESQL_ENGINE)

        ## Check samples table exists
        table = pd.read_sql("""
            SELECT EXISTS (
                SELECT 1 FROM pg_tables
                WHERE schemaname = 'public' AND tablename = '{}'
            )""".format('samplesqc'), db_engine)

        assert table.iloc[0, 0]

        samplesqc = pd.read_sql("select * from samplesqc order by eid asc",
                                create_engine(POSTGRESQL_ENGINE), index_col='eid')
        assert samplesqc is not None
        expected_columns = ['ccolumn_name_0_0', 'canothercolumn_0_0', 'cthird_column_0_0', 'cother_measure_col_umn_0_0']
        assert len(samplesqc.columns) == len(expected_columns)
        assert all(x in samplesqc.columns for x in expected_columns)

        assert not samplesqc.empty
        assert samplesqc.shape[0] == 4

        assert samplesqc.loc[10, 'ccolumn_name_0_0'] == 'UKBB'
        assert samplesqc.loc[10, 'canothercolumn_0_0'] == 'Batch'
        assert samplesqc.loc[10, 'cthird_column_0_0'] == 'SomeValue'
        assert samplesqc.loc[10, 'cother_measure_col_umn_0_0'] == 8.33992

        assert samplesqc.loc[20, 'ccolumn_name_0_0'] == 'Other'
        assert samplesqc.loc[20, 'canothercolumn_0_0'] == 'Some'
        assert samplesqc.loc[20, 'cthird_column_0_0'] == 'AnotherValue'
        assert samplesqc.loc[20, 'cother_measure_col_umn_0_0'] == -772.1234

        assert samplesqc.loc[30, 'ccolumn_name_0_0'] == 'Other12'
        assert samplesqc.loc[30, 'canothercolumn_0_0'] == 'Some12'
        assert samplesqc.loc[30, 'cthird_column_0_0'] == 'AnotherValue12'
        assert samplesqc.loc[30, 'cother_measure_col_umn_0_0'] == -0.000001234

        assert samplesqc.loc[2222240, 'ccolumn_name_0_0'] == 'Other13'
        assert samplesqc.loc[2222240, 'canothercolumn_0_0'] == 'Some13'
        assert samplesqc.loc[2222240, 'cthird_column_0_0'] == 'AnotherValue13'
        assert samplesqc.loc[2222240, 'cother_measure_col_umn_0_0'] == 0.051234

    def test_postload_load_samples_data_two_files(self):
        # prepare
        directory = get_repository_path('postloader/samples_data02')

        # run
        pl = Postloader(POSTGRESQL_ENGINE)
        pl.load_samples_data(directory)

        # Validate
        db_engine = create_engine(POSTGRESQL_ENGINE)

        # samplesqc
        table = pd.read_sql("""
            SELECT EXISTS (
                SELECT 1 FROM pg_tables
                WHERE schemaname = 'public' AND tablename = '{}'
            )""".format('samplesqc'), db_engine)

        assert table.iloc[0, 0]

        samplesqc = pd.read_sql("select * from samplesqc order by eid asc",
                                create_engine(POSTGRESQL_ENGINE), index_col='eid')
        assert samplesqc is not None
        expected_columns = ['ccolumn_name_0_0', 'canothercolumn_0_0', 'cpc1_0_0', 'cpc2_0_0']
        assert len(samplesqc.columns) == len(expected_columns)
        assert all(x in samplesqc.columns for x in expected_columns)

        assert not samplesqc.empty
        assert samplesqc.shape[0] == 2

        assert samplesqc.loc[10, 'ccolumn_name_0_0'] == 'UKBB'
        assert samplesqc.loc[10, 'canothercolumn_0_0'] == 'Batch'
        assert samplesqc.loc[10, 'cpc1_0_0'] == -1.76106
        assert samplesqc.loc[10, 'cpc2_0_0'] == 0.357072

        assert samplesqc.loc[2222240, 'ccolumn_name_0_0'] == 'Other13'
        assert samplesqc.loc[2222240, 'canothercolumn_0_0'] == 'Some13'
        assert samplesqc.loc[2222240, 'cpc1_0_0'] == 2.47186
        assert samplesqc.loc[2222240, 'cpc2_0_0'] == -5.46438

        # relatedness
        table = pd.read_sql("""
            SELECT EXISTS (
                SELECT 1 FROM pg_tables
                WHERE schemaname = 'public' AND tablename = '{}'
            )""".format('relatedness'), db_engine)

        assert table.iloc[0, 0]

        samplesqc = pd.read_sql("select * from relatedness order by eid asc",
                                create_engine(POSTGRESQL_ENGINE), index_col='eid')
        assert samplesqc is not None
        expected_columns = ['cid2_0_0', 'chethet_0_0', 'cibs0_0_0', 'ckinship_0_0']
        assert len(samplesqc.columns) == len(expected_columns)
        assert all(x in samplesqc.columns for x in expected_columns)

        assert not samplesqc.empty
        assert samplesqc.shape[0] == 3

        assert samplesqc.loc[10, 'cid2_0_0'] == 10
        assert samplesqc.loc[10, 'chethet_0_0'] == 0.016
        assert samplesqc.loc[10, 'cibs0_0_0'] == 0.0148
        assert samplesqc.loc[10, 'ckinship_0_0'] == 0.1367

        assert samplesqc.loc[20, 'cid2_0_0'] == 20
        assert samplesqc.loc[20, 'chethet_0_0'] == 0.02
        assert samplesqc.loc[20, 'cibs0_0_0'] == 0.0143
        assert samplesqc.loc[20, 'ckinship_0_0'] == 0.0801

        assert samplesqc.loc[2222240, 'cid2_0_0'] == 2222240
        assert samplesqc.loc[2222240, 'chethet_0_0'] == 0.038
        assert samplesqc.loc[2222240, 'cibs0_0_0'] == 0.0227
        assert samplesqc.loc[2222240, 'ckinship_0_0'] == 0.0742

    def test_postload_load_samples_data_no_eid_column(self):
        # prepare
        directory = get_repository_path('postloader/samples_data03')

        # run
        pl = Postloader(POSTGRESQL_ENGINE)
        pl.load_samples_data(directory)

        # Validate
        db_engine = create_engine(POSTGRESQL_ENGINE)

        # samplesqc
        table = pd.read_sql("""
            SELECT EXISTS (
                SELECT 1 FROM pg_tables
                WHERE schemaname = 'public' AND tablename = '{}'
            )""".format('samplesqc'), db_engine)

        assert not table.iloc[0, 0]

        # relatedness
        table = pd.read_sql("""
            SELECT EXISTS (
                SELECT 1 FROM pg_tables
                WHERE schemaname = 'public' AND tablename = '{}'
            )""".format('relatedness'), db_engine)

        assert not table.iloc[0, 0]

    def test_postload_load_samples_data_identifier_column_specified(self):
        # prepare
        directory = get_repository_path('postloader/samples_data03')

        # run
        pl = Postloader(POSTGRESQL_ENGINE)
        pl.load_samples_data(directory, {
            'relatedness.txt': 'ID1',
            'samplesqc.txt': 'ID',
        })

        # Validate
        db_engine = create_engine(POSTGRESQL_ENGINE)

        # samplesqc
        table = pd.read_sql("""
            SELECT EXISTS (
                SELECT 1 FROM pg_tables
                WHERE schemaname = 'public' AND tablename = '{}'
            )""".format('samplesqc'), db_engine)

        assert table.iloc[0, 0]

        samplesqc = pd.read_sql("select * from samplesqc order by eid asc",
                                create_engine(POSTGRESQL_ENGINE), index_col='eid')
        assert samplesqc is not None
        expected_columns = ['ccolumn_name_0_0', 'canothercolumn_0_0', 'cpc1_0_0', 'cpc2_0_0']
        assert len(samplesqc.columns) == len(expected_columns)
        assert all(x in samplesqc.columns for x in expected_columns)

        assert not samplesqc.empty
        assert samplesqc.shape[0] == 2

        assert samplesqc.loc[10, 'ccolumn_name_0_0'] == 'UKBB'
        assert samplesqc.loc[10, 'canothercolumn_0_0'] == 'Batch'
        assert samplesqc.loc[10, 'cpc1_0_0'] == -1.76106
        assert samplesqc.loc[10, 'cpc2_0_0'] == 0.357072

        assert samplesqc.loc[2222240, 'ccolumn_name_0_0'] == 'Other13'
        assert samplesqc.loc[2222240, 'canothercolumn_0_0'] == 'Some13'
        assert samplesqc.loc[2222240, 'cpc1_0_0'] == 2.47186
        assert samplesqc.loc[2222240, 'cpc2_0_0'] == -5.46438

        # relatedness
        table = pd.read_sql("""
            SELECT EXISTS (
                SELECT 1 FROM pg_tables
                WHERE schemaname = 'public' AND tablename = '{}'
            )""".format('relatedness'), db_engine)

        assert table.iloc[0, 0]

        samplesqc = pd.read_sql("select * from relatedness order by eid asc",
                                create_engine(POSTGRESQL_ENGINE), index_col='eid')
        assert samplesqc is not None
        expected_columns = ['cid2_0_0', 'chethet_0_0', 'cibs0_0_0', 'ckinship_0_0']
        assert len(samplesqc.columns) == len(expected_columns)
        assert all(x in samplesqc.columns for x in expected_columns)

        assert not samplesqc.empty
        assert samplesqc.shape[0] == 3

        assert samplesqc.loc[10, 'cid2_0_0'] == 10
        assert samplesqc.loc[10, 'chethet_0_0'] == 0.016
        assert samplesqc.loc[10, 'cibs0_0_0'] == 0.0148
        assert samplesqc.loc[10, 'ckinship_0_0'] == 0.1367

        assert samplesqc.loc[20, 'cid2_0_0'] == 20
        assert samplesqc.loc[20, 'chethet_0_0'] == 0.02
        assert samplesqc.loc[20, 'cibs0_0_0'] == 0.0143
        assert samplesqc.loc[20, 'ckinship_0_0'] == 0.0801

        assert samplesqc.loc[2222240, 'cid2_0_0'] == 2222240
        assert samplesqc.loc[2222240, 'chethet_0_0'] == 0.038
        assert samplesqc.loc[2222240, 'cibs0_0_0'] == 0.0227
        assert samplesqc.loc[2222240, 'ckinship_0_0'] == 0.0742

    def test_postload_load_samples_data_skip_column(self):
        # prepare
        directory = get_repository_path('postloader/samples_data03')

        # run
        pl = Postloader(POSTGRESQL_ENGINE)
        pl.load_samples_data(directory,
                 identifier_columns={
                     'relatedness.txt': 'ID1',
                     'samplesqc.txt': 'ID',
                 },
                 skip_columns={
                     'relatedness.txt': ['ID2'],
                     'samplesqc.txt': ['PC1', 'column.name'],
                 }
        )

        # Validate
        db_engine = create_engine(POSTGRESQL_ENGINE)

        # samplesqc
        table = pd.read_sql("""
            SELECT EXISTS (
                SELECT 1 FROM pg_tables
                WHERE schemaname = 'public' AND tablename = '{}'
            )""".format('samplesqc'), db_engine)

        assert table.iloc[0, 0]

        samplesqc = pd.read_sql("select * from samplesqc order by eid asc",
                                create_engine(POSTGRESQL_ENGINE), index_col='eid')
        assert samplesqc is not None
        expected_columns = ['canothercolumn_0_0', 'cpc2_0_0']
        assert len(samplesqc.columns) == len(expected_columns)
        assert all(x in samplesqc.columns for x in expected_columns)

        assert not samplesqc.empty
        assert samplesqc.shape[0] == 2

        assert samplesqc.loc[10, 'canothercolumn_0_0'] == 'Batch'
        assert samplesqc.loc[10, 'cpc2_0_0'] == 0.357072

        assert samplesqc.loc[2222240, 'canothercolumn_0_0'] == 'Some13'
        assert samplesqc.loc[2222240, 'cpc2_0_0'] == -5.46438

        # relatedness
        table = pd.read_sql("""
            SELECT EXISTS (
                SELECT 1 FROM pg_tables
                WHERE schemaname = 'public' AND tablename = '{}'
            )""".format('relatedness'), db_engine)

        assert table.iloc[0, 0]

        samplesqc = pd.read_sql("select * from relatedness order by eid asc",
                                create_engine(POSTGRESQL_ENGINE), index_col='eid')
        assert samplesqc is not None
        expected_columns = ['chethet_0_0', 'cibs0_0_0', 'ckinship_0_0']
        assert len(samplesqc.columns) == len(expected_columns)
        assert all(x in samplesqc.columns for x in expected_columns)

        assert not samplesqc.empty
        assert samplesqc.shape[0] == 3

        assert samplesqc.loc[10, 'chethet_0_0'] == 0.016
        assert samplesqc.loc[10, 'cibs0_0_0'] == 0.0148
        assert samplesqc.loc[10, 'ckinship_0_0'] == 0.1367

        assert samplesqc.loc[20, 'chethet_0_0'] == 0.02
        assert samplesqc.loc[20, 'cibs0_0_0'] == 0.0143
        assert samplesqc.loc[20, 'ckinship_0_0'] == 0.0801

        assert samplesqc.loc[2222240, 'chethet_0_0'] == 0.038
        assert samplesqc.loc[2222240, 'cibs0_0_0'] == 0.0227
        assert samplesqc.loc[2222240, 'ckinship_0_0'] == 0.0742

    def test_postload_load_samples_data_different_separators(self):
        # prepare
        directory = get_repository_path('postloader/samples_data04')

        # run
        pl = Postloader(POSTGRESQL_ENGINE)
        pl.load_samples_data(directory,
                 identifier_columns={
                     'relatedness.txt': 'ID1',
                     'samplesqc.txt': 'ID',
                 },
                 separators={
                     'relatedness.txt': '\t',
                     'samplesqc.txt': ',',
                 }
        )

        # Validate
        db_engine = create_engine(POSTGRESQL_ENGINE)

        # samplesqc
        table = pd.read_sql("""
            SELECT EXISTS (
                SELECT 1 FROM pg_tables
                WHERE schemaname = 'public' AND tablename = '{}'
            )""".format('samplesqc'), db_engine)

        assert table.iloc[0, 0]

        samplesqc = pd.read_sql("select * from samplesqc order by eid asc",
                                create_engine(POSTGRESQL_ENGINE), index_col='eid')
        assert samplesqc is not None
        expected_columns = ['ccolumn_name_0_0', 'canothercolumn_0_0', 'cpc1_0_0', 'cpc2_0_0']
        assert len(samplesqc.columns) == len(expected_columns)
        assert all(x in samplesqc.columns for x in expected_columns)

        assert not samplesqc.empty
        assert samplesqc.shape[0] == 2

        assert samplesqc.loc[10, 'ccolumn_name_0_0'] == 'UKBB'
        assert samplesqc.loc[10, 'canothercolumn_0_0'] == 'Batch'
        assert samplesqc.loc[10, 'cpc1_0_0'] == -1.76106
        assert samplesqc.loc[10, 'cpc2_0_0'] == 0.357072

        assert samplesqc.loc[2222240, 'ccolumn_name_0_0'] == 'Other13'
        assert samplesqc.loc[2222240, 'canothercolumn_0_0'] == 'Some13'
        assert samplesqc.loc[2222240, 'cpc1_0_0'] == 2.47186
        assert samplesqc.loc[2222240, 'cpc2_0_0'] == -5.46438

        # relatedness
        table = pd.read_sql("""
            SELECT EXISTS (
                SELECT 1 FROM pg_tables
                WHERE schemaname = 'public' AND tablename = '{}'
            )""".format('relatedness'), db_engine)

        assert table.iloc[0, 0]

        samplesqc = pd.read_sql("select * from relatedness order by eid asc",
                                create_engine(POSTGRESQL_ENGINE), index_col='eid')
        assert samplesqc is not None
        expected_columns = ['cid2_0_0', 'chethet_0_0', 'cibs0_0_0', 'ckinship_0_0']
        assert len(samplesqc.columns) == len(expected_columns)
        assert all(x in samplesqc.columns for x in expected_columns)

        assert not samplesqc.empty
        assert samplesqc.shape[0] == 3

        assert samplesqc.loc[10, 'cid2_0_0'] == 10
        assert samplesqc.loc[10, 'chethet_0_0'] == 0.016
        assert samplesqc.loc[10, 'cibs0_0_0'] == 0.0148
        assert samplesqc.loc[10, 'ckinship_0_0'] == 0.1367

        assert samplesqc.loc[20, 'cid2_0_0'] == 20
        assert samplesqc.loc[20, 'chethet_0_0'] == 0.02
        assert samplesqc.loc[20, 'cibs0_0_0'] == 0.0143
        assert samplesqc.loc[20, 'ckinship_0_0'] == 0.0801

        assert samplesqc.loc[2222240, 'cid2_0_0'] == 2222240
        assert samplesqc.loc[2222240, 'chethet_0_0'] == 0.038
        assert samplesqc.loc[2222240, 'cibs0_0_0'] == 0.0227
        assert samplesqc.loc[2222240, 'ckinship_0_0'] == 0.0742

    def test_postload_load_samples_fields_table_filled(self):
        # prepare
        postloader_directory = get_repository_path('postloader/samples_data04')
        pl = Postloader(POSTGRESQL_ENGINE)

        pheno_directory = get_repository_path('pheno2sql/example12')
        csv_file = get_repository_path(os.path.join(pheno_directory, 'example12_diseases.csv'))
        p2sql = Pheno2SQL(csv_file, POSTGRESQL_ENGINE, bgen_sample_file=os.path.join(pheno_directory, 'impv2.sample'),
                          n_columns_per_table=2, loading_n_jobs=1)

        # run
        p2sql.load_data()

        pl.load_samples_data(postloader_directory,
                 identifier_columns={
                     'relatedness.txt': 'ID1',
                     'samplesqc.txt': 'ID',
                 },
                 separators={
                     'relatedness.txt': '\t',
                     'samplesqc.txt': ',',
                 }
        )

        # Validate
        db_engine = create_engine(POSTGRESQL_ENGINE)

        # samplesqc
        table = pd.read_sql("""
            SELECT EXISTS (
                SELECT 1 FROM pg_tables
                WHERE schemaname = 'public' AND tablename = '{}'
            )""".format('samplesqc'), db_engine)

        assert table.iloc[0, 0]

        samplesqc = pd.read_sql("select * from samplesqc order by eid asc",
                                create_engine(POSTGRESQL_ENGINE), index_col='eid')
        assert samplesqc is not None
        expected_columns = ['ccolumn_name_0_0', 'canothercolumn_0_0', 'cpc1_0_0', 'cpc2_0_0']
        assert len(samplesqc.columns) == len(expected_columns)
        assert all(x in samplesqc.columns for x in expected_columns)

        assert not samplesqc.empty
        assert samplesqc.shape[0] == 2

        # check samplesqc columns are in fields table
        tmp = pd.read_sql("select * from fields where table_name = 'samplesqc'", db_engine, index_col='column_name')
        assert tmp is not None
        assert tmp.shape[0] == len(expected_columns)
        assert all(x in tmp.index.tolist() for x in expected_columns)

        assert tmp.loc['ccolumn_name_0_0', 'table_name'] == 'samplesqc'
        assert tmp.loc['ccolumn_name_0_0', 'field_id'] == 'ccolumn_name_0_0'
        assert tmp.loc['ccolumn_name_0_0', 'type'] == 'Text'

        assert tmp.loc['canothercolumn_0_0', 'table_name'] == 'samplesqc'
        assert tmp.loc['canothercolumn_0_0', 'field_id'] == 'canothercolumn_0_0'
        assert tmp.loc['canothercolumn_0_0', 'type'] == 'Text'

        assert tmp.loc['cpc1_0_0', 'table_name'] == 'samplesqc'
        assert tmp.loc['cpc1_0_0', 'field_id'] == 'cpc1_0_0'
        assert tmp.loc['cpc1_0_0', 'type'] == 'Continuous'

        assert tmp.loc['cpc2_0_0', 'table_name'] == 'samplesqc'
        assert tmp.loc['cpc2_0_0', 'field_id'] == 'cpc2_0_0'
        assert tmp.loc['cpc2_0_0', 'type'] == 'Continuous'


        # relatedness
        table = pd.read_sql("""
            SELECT EXISTS (
                SELECT 1 FROM pg_tables
                WHERE schemaname = 'public' AND tablename = '{}'
            )""".format('relatedness'), db_engine)

        assert table.iloc[0, 0]

        samplesqc = pd.read_sql("select * from relatedness order by eid asc",
                                create_engine(POSTGRESQL_ENGINE), index_col='eid')
        assert samplesqc is not None
        expected_columns = ['cid2_0_0', 'chethet_0_0', 'cibs0_0_0', 'ckinship_0_0']
        assert len(samplesqc.columns) == len(expected_columns)
        assert all(x in samplesqc.columns for x in expected_columns)

        assert not samplesqc.empty
        assert samplesqc.shape[0] == 3

        # check relatedness columns are in fields table
        tmp = pd.read_sql("select * from fields where table_name = 'relatedness'", db_engine, index_col='column_name')
        assert tmp is not None
        assert tmp.shape[0] == len(expected_columns)
        assert all(x in tmp.index.tolist() for x in expected_columns)

        assert tmp.loc['cid2_0_0', 'table_name'] == 'relatedness'
        assert tmp.loc['cid2_0_0', 'field_id'] == 'cid2_0_0'
        assert tmp.loc['cid2_0_0', 'type'] == 'Integer'

        assert tmp.loc['chethet_0_0', 'table_name'] == 'relatedness'
        assert tmp.loc['chethet_0_0', 'field_id'] == 'chethet_0_0'
        assert tmp.loc['chethet_0_0', 'type'] == 'Continuous'

        assert tmp.loc['cibs0_0_0', 'table_name'] == 'relatedness'
        assert tmp.loc['cibs0_0_0', 'field_id'] == 'cibs0_0_0'
        assert tmp.loc['cibs0_0_0', 'type'] == 'Continuous'

        assert tmp.loc['ckinship_0_0', 'table_name'] == 'relatedness'
        assert tmp.loc['ckinship_0_0', 'field_id'] == 'ckinship_0_0'
        assert tmp.loc['ckinship_0_0', 'type'] == 'Continuous'

    def test_postload_samples_data_check_constrains_exist(self):
        # prepare
        directory = get_repository_path('postloader/samples_data04')

        # run
        pl = Postloader(POSTGRESQL_ENGINE)
        pl.load_samples_data(directory,
                 identifier_columns={
                     'relatedness.txt': 'ID1',
                     'samplesqc.txt': 'ID',
                 },
                 separators={
                     'relatedness.txt': '\t',
                     'samplesqc.txt': ',',
                 }
        )

        # Validate
        ## Check samplesqc table exists
        table = pd.read_sql("""
            SELECT EXISTS (
                SELECT 1 FROM pg_tables
                WHERE schemaname = 'public' AND tablename = '{}'
            )""".format('samplesqc'),
                            create_engine(POSTGRESQL_ENGINE))

        assert table.iloc[0, 0]

        # primary key
        constraint_sql = self._get_table_contrains('samplesqc', relationship_query='pk_%%')
        constraints_results = pd.read_sql(constraint_sql, create_engine(POSTGRESQL_ENGINE))
        assert constraints_results is not None
        assert not constraints_results.empty
        columns = constraints_results['column_name'].tolist()
        assert len(columns) == 1
        assert 'eid' in columns

        ## Check relatedness table exists
        table = pd.read_sql("""
            SELECT EXISTS (
                SELECT 1 FROM pg_tables
                WHERE schemaname = 'public' AND tablename = '{}'
            )""".format('relatedness'),
                            create_engine(POSTGRESQL_ENGINE))

        assert table.iloc[0, 0]

        # primary key
        constraint_sql = self._get_table_contrains('relatedness', relationship_query='pk_%%')
        constraints_results = pd.read_sql(constraint_sql, create_engine(POSTGRESQL_ENGINE))
        assert constraints_results is not None
        assert not constraints_results.empty
        columns = constraints_results['column_name'].tolist()
        assert len(columns) == 1
        assert 'eid' in columns

    def test_postload_codings_table_many_tab_characters_and_na(self):
        # prepare
        directory = get_repository_path('postloader/codings04_many_tabs')

        # run
        pl = Postloader(POSTGRESQL_ENGINE)
        pl.load_codings(directory)

        # validate
        ## Check samples table exists
        table = pd.read_sql("""
            SELECT EXISTS (
                SELECT 1 FROM pg_tables
                WHERE schemaname = 'public' AND tablename = '{}'
            )""".format('codings'),
                            create_engine(POSTGRESQL_ENGINE))

        assert table.iloc[0, 0]

        codings = pd.read_sql("select * from codings order by data_coding, coding", create_engine(POSTGRESQL_ENGINE))
        assert codings is not None
        expected_columns = ['data_coding', 'coding', 'meaning']
        assert len(codings.columns) >= len(expected_columns)
        assert all(x in codings.columns for x in expected_columns)

        assert not codings.empty
        assert codings.shape[0] == 5

        cidx = 0
        assert codings.loc[cidx, 'data_coding'] == 7
        assert codings.loc[cidx, 'coding'] == '0'
        assert codings.loc[cidx, 'meaning'] == 'No'

        cidx += 1
        assert codings.loc[cidx, 'data_coding'] == 7
        assert codings.loc[cidx, 'coding'] == '1'
        assert codings.loc[cidx, 'meaning'] == 'Yes'

        cidx += 1
        assert codings.loc[cidx, 'data_coding'] == 9
        assert codings.loc[cidx, 'coding'] == '0'
        assert codings.loc[cidx, 'meaning'] == 'Female'

        cidx += 1
        assert codings.loc[cidx, 'data_coding'] == 9
        assert codings.loc[cidx, 'coding'] == '1'
        assert codings.loc[cidx, 'meaning'] == 'Male'

        cidx += 1
        assert codings.loc[cidx, 'data_coding'] == 9
        assert codings.loc[cidx, 'coding'] == '2'
        assert codings.loc[cidx, 'meaning'] == 'N/A'

    def test_postload_load_samples_data_multiple_identifiers(self):
        # prepare
        directory = get_repository_path('postloader/samples_data05_multiple_identifiers')

        # run
        pl = Postloader(POSTGRESQL_ENGINE)
        pl.load_samples_data(directory,
                 identifier_columns={
                     'relatedness.txt': ['ID1', 'ID2'],
                     'samplesqc.txt': 'ID',
                 },
                 separators={
                     'relatedness.txt': '\t',
                     'samplesqc.txt': ',',
                 },
                 skip_columns={
                     'samplesqc.txt': ['PC1', 'column.name'],
                 }
        )

        # Validate
        db_engine = create_engine(POSTGRESQL_ENGINE)

        # samplesqc
        table = pd.read_sql("""
            SELECT EXISTS (
                SELECT 1 FROM pg_tables
                WHERE schemaname = 'public' AND tablename = '{}'
            )""".format('samplesqc'), db_engine)

        assert table.iloc[0, 0]

        samplesqc = pd.read_sql("select * from samplesqc order by eid asc",
                                create_engine(POSTGRESQL_ENGINE), index_col='eid')
        assert samplesqc is not None
        expected_columns = ['canothercolumn_0_0', 'cpc2_0_0']
        assert len(samplesqc.columns) == len(expected_columns)
        assert all(x in samplesqc.columns for x in expected_columns)

        assert not samplesqc.empty
        assert samplesqc.shape[0] == 2

        assert samplesqc.loc[10, 'canothercolumn_0_0'] == 'Batch'
        assert samplesqc.loc[10, 'cpc2_0_0'] == 0.357072

        assert samplesqc.loc[2222240, 'canothercolumn_0_0'] == 'Some13'
        assert samplesqc.loc[2222240, 'cpc2_0_0'] == -5.46438

        # relatedness
        table = pd.read_sql("""
            SELECT EXISTS (
                SELECT 1 FROM pg_tables
                WHERE schemaname = 'public' AND tablename = '{}'
            )""".format('relatedness'), db_engine)

        assert table.iloc[0, 0]

        samplesqc = pd.read_sql("select * from relatedness order by id1 asc, id2 asc",
                                create_engine(POSTGRESQL_ENGINE), index_col=['id1', 'id2'])
        assert samplesqc is not None
        expected_columns = ['chethet_0_0', 'cibs0_0_0', 'ckinship_0_0']
        assert len(samplesqc.columns) == len(expected_columns)
        assert all(x in samplesqc.columns for x in expected_columns)

        assert not samplesqc.empty
        assert samplesqc.shape[0] == 6

        assert samplesqc.loc[10].loc[10, 'chethet_0_0'] == 0.016
        assert samplesqc.loc[10].loc[10, 'cibs0_0_0'] == 0.0148
        assert samplesqc.loc[10].loc[10, 'ckinship_0_0'] == 0.1367

        assert samplesqc.loc[10].loc[20, 'chethet_0_0'] == 0.316
        assert samplesqc.loc[10].loc[20, 'cibs0_0_0'] == 0.9148
        assert samplesqc.loc[10].loc[20, 'ckinship_0_0'] == 0.0667

        assert samplesqc.loc[20].loc[20, 'chethet_0_0'] == 0.02
        assert samplesqc.loc[20].loc[20, 'cibs0_0_0'] == 0.0143
        assert samplesqc.loc[20].loc[20, 'ckinship_0_0'] == 0.0801

        assert samplesqc.loc[2222240].loc[2222240, 'chethet_0_0'] == 0.038
        assert samplesqc.loc[2222240].loc[2222240, 'cibs0_0_0'] == 0.0227
        assert samplesqc.loc[2222240].loc[2222240, 'ckinship_0_0'] == 0.0742

        assert samplesqc.loc[2222240].loc[10, 'chethet_0_0'] == 0.138
        assert samplesqc.loc[2222240].loc[10, 'cibs0_0_0'] == 0.1227
        assert samplesqc.loc[2222240].loc[10, 'ckinship_0_0'] == 0.1742

        assert samplesqc.loc[2222240].loc[20, 'chethet_0_0'] == 0.238
        assert samplesqc.loc[2222240].loc[20, 'cibs0_0_0'] == 0.2227
        assert samplesqc.loc[2222240].loc[20, 'ckinship_0_0'] == 0.2742
