import pandas as pd
import numpy as np
import re
from sqlalchemy.exc import ProgrammingError
from ukbrest.config import logger

from ukbrest.common.utils.constants import ALL_EIDS_TABLE
from ukbrest.common.utils.db import DBAccess
from ukbrest.common.utils.misc import get_list

from ukbrest.resources.exceptions import (UkbRestSQLExecutionError,
                                          UkbRestProgramExecutionError)


class YAMLQuery(DBAccess):
    _RE_COLUMN_NAME_PATTERN = '(?i)c[0-9a-z_]+_[0-9]+_[0-9]+'
    RE_COLUMN_NAME = re.compile('({})'.format(_RE_COLUMN_NAME_PATTERN))

    _RE_FULL_COLUMN_NAME_RENAME_PATTERN = '^(?i)\(?(?P<field>{})\)?([ ]+([ ]*as[ ]+)?(?P<rename>[\w_]+))?$'.format(_RE_COLUMN_NAME_PATTERN)
    RE_FULL_COLUMN_NAME_RENAME = re.compile(_RE_FULL_COLUMN_NAME_RENAME_PATTERN)

    def __init__(self, db_uri, sql_chunksize ):
        super(YAMLQuery, self).__init__(db_uri)
        self.sql_chunksize = sql_chunksize

    def _query_generic(self, sql_query, order_by_dict=None, results_transformator=None):
        final_sql_query = sql_query

        if order_by_dict is not None:
            outer_sql = """
                select {data_fields}
                from {order_by} s left outer join (
                    {base_sql}
                ) u
                using (eid)
                order by s.index asc
            """.format(
                order_by=order_by_dict['table'],
                base_sql=sql_query,
                data_fields=order_by_dict['columns_select']
            )

            final_sql_query = outer_sql

        logger.debug(final_sql_query)

        try:
            results_iterator = pd.read_sql(
                final_sql_query, self._get_db_engine(), index_col='eid',
                chunksize=self.sql_chunksize
            )
        except ProgrammingError as e:
            raise UkbRestSQLExecutionError(str(e))

        if self.sql_chunksize is None:
            results_iterator = iter([results_iterator])

        for chunk in results_iterator:
            if results_transformator is not None:
                chunk = results_transformator(chunk)

            yield chunk

    def _get_fields_from_reg_exp(self, ecolumns):
        if ecolumns is None:
            return []

        where_st = ["column_name ~ '{}'".format(ecol) for ecol in ecolumns]
        select_st = """
            select distinct column_name
            from fields
            where {}
            order by column_name
        """.format(' or '.join(where_st))

        return pd.read_sql(select_st, self._get_db_engine()).loc[:, 'column_name'].tolist()

    @staticmethod
    def _get_fields_from_statements(statement):
        """This method gets all fields mentioned in the statements."""
        columns_fields = []
        if statement is not None:
            columns_fields = list(set([x for col in statement for x in re.findall(YAMLQuery.RE_COLUMN_NAME, col)]))

        return columns_fields

    def _get_needed_tables(self, all_columns):
        if len(all_columns) == 0:
            return []

        all_columns_quoted = ["'{}'".format(x.replace("'", "''")) for x in all_columns]

        tables_needed_df = pd.read_sql(
            'select distinct table_name '
            'from fields '
            'where column_name in (' + ','.join(all_columns_quoted) + ')',
        self._get_db_engine()).loc[:, 'table_name'].tolist()

        if len(tables_needed_df) == 0:
            return []

        return tables_needed_df


    def _get_query_sql(self, columns=None, ecolumns=None, filterings=None):
        # select needed tables to join
        columns_fields = self._get_fields_from_statements(columns)
        reg_exp_columns_fields = self._get_fields_from_reg_exp(ecolumns)
        filterings_columns_fields = self._get_fields_from_statements(filterings)

        tables_needed_df = self._get_needed_tables(columns_fields + reg_exp_columns_fields + filterings_columns_fields)

        all_columns = ['eid'] + (columns if columns is not None else []) + reg_exp_columns_fields

        base_sql = """
            select {data_fields}
            {from_clause}
            {where_statements}
        """

        tables_join_sql = self._create_joins(tables_needed_df, join_type='full outer join')

        if tables_join_sql:
            from_clause_sql = f'from {tables_join_sql}'
        else:
            from_clause_sql = 'from all_eids'


        return base_sql.format(
            data_fields=','.join(all_columns),
            from_clause=from_clause_sql,
            where_statements=((' where ' + self._get_filterings(filterings)) if filterings is not None else ''),
        )

    def _get_integer_fields(self, columns):
        """This method returns a list of fields (either its column specification, like c64_0_0 or its rename like
        myfield) that are of type integer."""
        int_columns = []

        for col in columns:
            if col == 'eid':
                continue

            match = re.search(YAMLQuery.RE_FULL_COLUMN_NAME_RENAME, col)

            if match is None:
                continue

            col_field = match.group('field')

            if self.get_field_dtype(col_field) != 'Integer':
                continue

            # select rename first, if not specified select field column
            col_rename = next((grp_val for grp_val in (match.group('rename'), match.group('field')) if grp_val is not None))
            int_columns.append(col_rename)

        return int_columns

    def query(self, columns=None, ecolumns=None, filterings=None, order_by_table=None):
        reg_exp_columns_fields = self._get_fields_from_reg_exp(ecolumns)
        all_columns = ['eid'] + (columns if columns is not None else []) + reg_exp_columns_fields

        order_by_dict = None
        if order_by_table is not None:
            order_by_dict = {
                'table': order_by_table,
                'columns_select': ','.join(all_columns),
            }

        int_columns = self._get_integer_fields(all_columns)

        final_sql_query = self._get_query_sql(columns, ecolumns, filterings)

        def format_integer_columns(chunk):
            for col in int_columns:
                chunk[col] = chunk[col].map(lambda x: np.nan if pd.isnull(x) else '{:1.0f}'.format(x))

            return chunk

        return self._query_generic(
            final_sql_query,
            results_transformator=format_integer_columns,
            order_by_dict=order_by_dict
        )

    @staticmethod
    def _get_filterings(filter_statements):
        return ' AND '.join('({})'.format(afilter) for afilter in filter_statements)


class PhenoQuery(YAMLQuery):
    def __init__(self, db_uri, sql_chunksize):
        super(PhenoQuery, self).__init__(db_uri, sql_chunksize)

    def query_yaml(self, yaml_file, section, order_by_table=None):
        if section.startswith('simple_'):
            return self.query_yaml_simple_data(yaml_file, section, order_by_table)
        else:
            return self.query_yaml_data(yaml_file, section, order_by_table)

    def query_yaml_data(self, yaml_file, section, order_by_table=None):
        all_columns = []
        all_columns_sql_queries = []

        where_st = ''
        where_fields = []
        if 'samples_filters' in yaml_file:
            where_st = self._get_filterings(yaml_file['samples_filters'])
            where_fields = self._get_fields_from_statements([where_st])

        for column, column_dict in yaml_file[section].items():
            all_columns.append(column)

            subqueries = []

            if isinstance(column_dict, dict):
                for df, df_cods in column_dict.items():
                    if df == 'sql':
                        for cat_code, cat_condition in df_cods.items():
                            # TODO: check for repeated category codes
                            needed_tables = self._get_needed_tables(
                                self._get_fields_from_statements([cat_condition]) + where_fields
                            )

                            sql_code = """
                                select eid, {cat_code} as {column_name}
                                from {cases_joins}
                                {where_st}
                            """.format(
                                    cat_code=cat_code,
                                    column_name=column,
                                    cases_joins=self._create_joins(needed_tables + [ALL_EIDS_TABLE]),
                                    where_st='where ({}) {}'.format(cat_condition, (' AND ({})'.format(where_st) if where_st else ''))
                            )

                            subqueries.append(sql_code)

                    elif df == 'case_control':
                        cases_conditions = [
                            '(field_id = {} and event in ({}))'.format(
                                field_id, ', '.join("'{}'".format(cod) for cod in get_list(field_cond['coding']))
                            ) for field_id, field_cond in df_cods.items()
                        ]

                        sql_cases = """
                            select distinct eid
                            from events
                            where {conditions}
                        """.format(conditions=' OR '.join(cases_conditions))

                        sql_cases_code = """
                                select eid, 1 as {column_name}
                                from {cases_joins}
                                {where_st}
                        """.format(
                            column_name=column,
                            cases_joins=self._create_joins([
                                '({}) ev'.format(sql_cases)
                            ] + self._get_needed_tables(where_fields)),
                            where_st='where ' + (where_st if where_st else '1=1')
                        )

                        subqueries.append(sql_cases_code)

                        # controls
                        sql_controls_code = """
                            select aet.eid, 0 as {column_name}
                            from {controls_joins}
                            {where_st}
                            and aet.eid not in (
                                {sql_cases}
                            )
                        """.format(
                            column_name=column,
                            controls_joins=self._create_joins([
                                '{} aet'.format(ALL_EIDS_TABLE),
                            ] + self._get_needed_tables(where_fields)),
                            where_st='where ' + (where_st if where_st else '1=1'),
                            sql_cases=sql_cases,
                        )

                        subqueries.append(sql_controls_code)

                    else:
                        raise Exception('Invalid selector type')

            elif isinstance(column_dict, str):
                final_sql = self._get_query_sql(
                    columns=['({}) as {}'.format(column_dict, column)],
                    filterings=yaml_file['samples_filters'] if 'samples_filters' in yaml_file else None,
                )

                subqueries.append(final_sql)
            else:
                raise Exception('Invalid query type')

            column_sql_query = ' union distinct '.join(subqueries)

            all_columns_sql_queries.append(column_sql_query)

        order_by_dict = None
        if order_by_table is not None:
            order_by_dict = {
                'table': order_by_table,
                'columns_select':
                    's.eid as eid, ' +
                    ', '.join('{column_name}::text'.format(column_name=column) for column in all_columns),
            }

        final_sql_query = """
            select eid, {columns_names}
            from {inner_queries}
        """.format(
            columns_names=', '.join('{}::text'.format(column) for column in all_columns),
            inner_queries=self._create_joins(
                ['({}) iq{}'.format(iq, iq_idx) for iq_idx, iq in enumerate(all_columns_sql_queries)],
                join_type='full outer join'
            ),
        )

        return self._query_generic(
            final_sql_query,
            order_by_dict=order_by_dict
        )

    def query_yaml_simple_data(self, yaml_file, section, order_by_table=None):
        section_data = yaml_file[section]

        include_only_stmts = None
        if 'samples_filters' in yaml_file:
            include_only_stmts = yaml_file['samples_filters']

        section_field_statements = ['({}) as {}'.format(v, x) for x, v in section_data.items()]

        for chunk in self.query(section_field_statements, filterings=include_only_stmts, order_by_table=order_by_table):
            # chunk = chunk.rename(columns={v:k for x in section_data.items()})
            yield chunk



class EHRQuery(YAMLQuery):
    def __init__(self, db_uri, sql_chunksize):
        super(EHRQuery, self).__init__(db_uri, sql_chunksize)

    def query_yaml(self, yaml_file):
        where_st = ''
        where_fields = []
        if 'samples_filters' in yaml_file:
            where_st = self._get_filterings(yaml_file['samples_filters'])
            # where_fields = self._get_fields_from_statements([where_st])


