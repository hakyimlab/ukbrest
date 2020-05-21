import csv
import os
import re
import sys
import tempfile
from subprocess import Popen, PIPE
from urllib.parse import urlparse

import numpy as np
import pandas as pd
from joblib import Parallel, delayed
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.types import TEXT, FLOAT, TIMESTAMP, INT
from sqlalchemy.exc import OperationalError

from ukbrest.common.utils.db import create_table, create_indexes
from ukbrest.common.utils.datagen import get_tmpdir
from ukbrest.common.utils.constants import BGEN_SAMPLES_TABLE, ALL_EIDS_TABLE
from ukbrest.config import logger, SQL_CHUNKSIZE_ENV
from ukbrest.common.loadsql import LoadSQL
from ukbrest.common.utils.misc import get_list
from ukbrest.resources.exceptions import UkbRestSQLExecutionError, UkbRestProgramExecutionError

class EHR2SQL(LoadSQL):
    K_CLINICAL = "gp_clinical"
    K_SCRIPTS = "gp_scripts"
    K_REGISTRATIONS = "gp_registrations"
    DD_GP = {K_CLINICAL :"gp_clinical.txt",
             K_SCRIPTS :"gp_scripts.txt",
             K_REGISTRATIONS :"gp_registrations.txt"}
    K_HESIN = "hesin"
    K_DIAG = "hesin_diag"
    DD_HESIN = {K_HESIN :"hesin.txt",
                K_DIAG :"hesin_diag.txt"}

    def __init__(self, db_uri, primary_care_dir, hospital_inpatient_dir,
                 n_columns_per_table=sys.maxsize,
                 loading_n_jobs=-1, tmpdir=tempfile.mkdtemp(prefix='ukbrest'),
                 loading_chunksize=5000, sql_chunksize=None,
                 delete_temp_csv=True):

        super(EHR2SQL, self).__init__(db_uri,
                                      n_columns_per_table=n_columns_per_table,
                                      loading_n_jobs=loading_n_jobs,
                                      tmpdir=tmpdir,
                                      loading_chunksize=loading_chunksize,
                                      sql_chunksize=sql_chunksize,
                                      delete_temp_csv=delete_temp_csv)
        if (hospital_inpatient_dir is None and
                primary_care_dir is None):
            raise ValueError("Neither hospital inpatient nor primary care "
                             "directories were specified.")
        self.primary_care_dir = primary_care_dir
        if self.primary_care_dir is not None:
            self.gp_file_dd = {}
            for k ,v in EHR2SQL.DD_GP.items():
                fp = os.path.join(self.primary_care_dir, v)
                if os.path.isfile(fp):
                    self.gp_file_dd[k] = fp
                else:
                    raise ValueError(v + " out of place")
        else:
            self.gp_file_dd = None
        self.hospital_inpatient_dir = hospital_inpatient_dir
        if self.hospital_inpatient_dir is not None:
            self.hesin_file_dd = {}
            for k ,v in EHR2SQL.DD_HESIN.items():
                fp = os.path.join(self.hospital_inpatient_dir, v)
                if os.path.isfile(fp):
                    self.hesin_file_dd[k] = fp
                else:
                    raise ValueError(v + " out of place")
        else:
            self.hesin_file_dd = None

    def load_data(self, vacuum=False):
        logger.info("Loading EHR into database")
        try:
            self._load_primary_care_data()
            self._load_hospital_inpatient_data()
            self._load_all_eids()
            self._create_constraints()

            if vacuum:
                self._vacuum()
        except OperationalError as e:
            raise UkbRestSQLExecutionError('There was an error with the database: ' + str(e))
        except UnicodeDecodeError as e:
            logger.debug(str(e))
            raise UkbRestProgramExecutionError('Unicode decoding error when reading CSV file. Activate debug to show more details.')

        logger.info("EHR loading finished")


    def _create_constraints(self):
        if self.db_type == 'sqlite':
            logger.warning("Indexes are not supported for SQLite")
            return

        logger.info("Creating table constraints (indexes, primary keys, etc)")

        create_indexes(EHR2SQL.K_CLINICAL,
                       ("eid", "read_2", "read_3"), #TODO: Add indexes?
                       db_engine=self._get_db_engine())
        create_indexes(EHR2SQL.K_SCRIPTS,
                       ("eid", "bnf_code", "dmd_code"),
                       db_engine=self._get_db_engine())

        create_indexes(EHR2SQL.K_REGISTRATIONS,
                       ("eid",),
                       db_engine=self._get_db_engine())
        create_indexes(EHR2SQL.K_HESIN,
                       ("eid",),
                       db_engine=self._get_db_engine())
        create_indexes(EHR2SQL.K_DIAG,
                       ("eid", "diag_icd9", "diag_icd10"),
                       db_engine=self._get_db_engine())

    @staticmethod
    def _load_ehr_df(fp, pk_cols, day_date_cols=None, month_date_cols=None,
                     ymd_date_cols=None, encoding=None,
                     accumulate_col_dict=None, extra_not_null = None,
                     dtype_handling=None):
        """

        :param fp: filepath to EHR text file
        :param pk_cols: columns treated as primary keys. Uniqueness and NonNull
                      are enforced.
        :param day_date_cols: Date columns to be treated with day first
        :param month_date_cols: Date columns to be treated with month first
        :param encoding: Text file encoding
        :param accumulate_col_dict: {resulting col name: [ first_option,
                                                            second_option,
                                                            ...]}
        :param extra_not_null: list of other Not Null columns
        :return: pandas DataFrame
        """
        logger.info("Loading table {}".format(fp))
        df = pd.read_table(fp, encoding=encoding, dtype=dtype_handling)
        if day_date_cols is not None:
            for c in day_date_cols:
                df[c] = pd.to_datetime(df[c], dayfirst=True)
        if month_date_cols is not None:
            for c in month_date_cols:
                df[c] = pd.to_datetime(df[c], monthfirst=True)
        if ymd_date_cols is not None:
            for c in ymd_date_cols:
                df[c] = pd.to_datetime(df[c], format="%Y%m%d")

        if accumulate_col_dict is not None:
            for col, fill_cols in accumulate_col_dict.items():
                df[col] = np.nan
                for i in fill_cols:
                    df[col] = df[col].fillna(df[i])

        l_0 = df.shape[0]
        df = df.drop_duplicates(pk_cols)
        l_1 = df.shape[0]
        if l_0 != l_1:
            logger.warning("Dropped {} rows due to uniqueness constraint".format(l_0 - l_1))
        if extra_not_null is not None:
            pk_cols.extend(extra_not_null)
        df = df.dropna(subset=pk_cols)
        l_2 = df.shape[0]
        if l_1 != l_2:
            logger.warning("Dropped {} rows due to nonnull constraint".format(l_1 - l_2))

        return df

    def _load_primary_care_data(self):

        db_engine = self._get_db_engine()

        # PG REGISTRATIONS
        self._create_gp_registrations_table()
        gp_registrations_df = self._load_ehr_df(self.gp_file_dd[EHR2SQL.K_REGISTRATIONS],
                                                ['eid', 'reg_date'],
                                                day_date_cols=['reg_date', 'deduct_date'])
        gp_registrations_df.to_sql(EHR2SQL.K_REGISTRATIONS, db_engine,
                                   if_exists='append', index=False)

        # PG CLINICAL
        self._create_gp_clinical_table()
        gp_clinical_df = self._load_ehr_df(self.gp_file_dd[EHR2SQL.K_CLINICAL],
                                           ['eid', 'event_dt', 'read_key'],
                                           day_date_cols=['event_dt'],
                                           accumulate_col_dict={'read_key' :['read_2', 'read_3']})
        gp_clinical_df.to_sql(EHR2SQL.K_CLINICAL, db_engine, if_exists='append',
                              index=False)

        # PG SCRIPTS
        self._create_gp_scripts_table()
        gp_scripts_df = self._load_ehr_df(self.gp_file_dd[EHR2SQL.K_SCRIPTS],
                                          ['eid', 'issue_date', 'read_key'],
                                          day_date_cols=['issue_date'],
                                          accumulate_col_dict={'read_key' :['bnf_code', 'dmd_code', 'read_2']},
                                          dtype_handling={'bnf_code' :str})
        gp_scripts_df.to_sql(EHR2SQL.K_SCRIPTS, db_engine, if_exists='append',
                             index=False)

    def _create_gp_scripts_table(self):
        create_table(EHR2SQL.K_SCRIPTS, columns = [
            'eid bigint NOT NULL',
            'data_provider int NOT NULL',
            'issue_date date NOT NULL',
            'read_key text NOT NULL',
            'read_2 text',
            'bnf_code text',
            'dmd_code text',
            'drug_name text',
            'quantity text'
        ],
                     constraints=[
                         'pk_{} PRIMARY KEY (eid, issue_date, read_key)'.format(EHR2SQL.K_SCRIPTS)
                     ],
                     db_engine=self._get_db_engine())

    def _create_gp_clinical_table(self):
        create_table(EHR2SQL.K_CLINICAL, columns=[
            'eid bigint NOT NULL',
            'data_provider int NOT NULL',
            'event_dt date NOT NULL',
            'read_key text NOT NULL',
            'read_2 text',
            'read_3 text',
            'value1 text',
            'value2 text',
            'value3 text',
        ],
                     constraints=[
                         'pk_{} PRIMARY KEY (eid, event_dt, read_key)'.format(EHR2SQL.K_CLINICAL)
                     ],
                     db_engine=self._get_db_engine())

    def _create_gp_registrations_table(self):
        create_table(EHR2SQL.K_REGISTRATIONS, columns=[
            'eid bigint NOT NULL',
            'data_provider int NOT NULL',
            'reg_date date NOT NULL',
            'deduct_date date',
        ],
                     constraints=[
                         'pk_{} PRIMARY KEY (eid, reg_date)'.format(EHR2SQL.K_REGISTRATIONS)
                     ],
                     db_engine=self._get_db_engine())

    def _load_hospital_inpatient_data(self):
        db_engine = self._get_db_engine()

        # HESIN
        self._create_hesin_table()
        hesin_df = self._load_ehr_df(self.hesin_file_dd[EHR2SQL.K_HESIN],
                                     ['eid', 'ins_index'],
                                     ymd_date_cols=['epistart', 'epiend',
                                                    'elecdate', 'admidate'])
        hesin_df.to_sql(EHR2SQL.K_HESIN, db_engine, if_exists='append',
                        index=False)

        # HESIN_DIAG
        self._create_hesin_diag_table()
        diag_df = self._load_ehr_df(self.hesin_file_dd[EHR2SQL.K_DIAG],
                                    ['eid', 'ins_index', 'arr_index'])
        diag_df.to_sql(EHR2SQL.K_DIAG, db_engine, if_exists='append',
                       index=False)

    def _create_hesin_table(self):
        create_table(EHR2SQL.K_HESIN,
                     ['eid bigint NOT NULL',
                      'ins_index bigint NOT NULL',
                      'dsource text NOT NULL',
                      'source text NOT NULL',
                      'epistart date',
                      'epiend date',
                      'epidur bigint',
                      'bedyear int',
                      'epistat int',
                      'epitype int',
                      'epiorder int',
                      'spell_index int',
                      'spell_seq int',
                      'spelbgin int',
                      'spelend int',
                      'speldur bigint',
                      'pctcode text',
                      'gpprpct text',
                      'category int',
                      'elecdate date',
                      'elecdur int',
                      'admidate date',
                      'admimeth_uni int',
                      'admimeth int',
                      'admisorc_uni int',
                      'admisorc int',
                      'firstreg int',
                      'classpat_uni int',
                      'classpat int',
                      'intmanag_uni int',
                      'intmanag int',
                      'mainspef_uni int',
                      'mainspef text',
                      'tretspef_uni int',
                      'tretspef text',
                      'operstat int',
                      'disdate date',
                      'dismeth_uni int',
                      'dismeth int',
                      'disdest_uni int',
                      'disdest int',
                      'carersi int'],
                     db_engine=self._get_db_engine(),
                     constraints=['pk_{} PRIMARY KEY (eid, ins_index)'.format(EHR2SQL.K_HESIN)])

    def _create_hesin_diag_table(self):
        create_table(EHR2SQL.K_DIAG, columns=[
            'eid bigint NOT NULL',
            'ins_index bigint NOT NULL',
            'arr_index bigint NOT NULL',
            'level int',
            'diag_icd9 text',
            'diag_icd9_nb text',
            'diag_icd10 text',
            'diag_icd10_nb text'],
                     db_engine=self._get_db_engine(),
                     constraints=['pk_{} PRIMARY KEY (eid, ins_index, arr_index)'.format(EHR2SQL.K_DIAG)])





