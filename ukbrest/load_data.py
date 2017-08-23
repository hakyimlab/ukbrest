import argparse
import tempfile
from ipaddress import ip_address

from ukbrest.common.pheno2sql import Pheno2SQL
from ukbrest import config

parser = argparse.ArgumentParser()
parser.add_argument('genotype_path', type=str, help='UK Biobank genotype (imputed) path')
parser.add_argument('phenotype_csv', type=str, nargs='+', help='UK Biobank phenotype data in CSV format')
parser.add_argument('--tmp_dir', dest='tmp_dir', type=str, required=False, help='Directory where write temporal files',
                    default=tempfile.gettempdir())
parser.add_argument('--db_uri', dest='db_uri', type=str, required=False, help='Database engine URI',
                    default='sqlite:///ukbrest_tmp.db')
parser.add_argument('--host', dest='host', type=ip_address, required=False, help='Host',
                    default=ip_address('127.0.0.1'))
parser.add_argument('--port', dest='port', type=int, required=False, help='Port', default=5000)
parser.add_argument('--debug', dest='debug', action='store_true')
parser.add_argument('--load_db', dest='load_db', action='store_true')


if __name__ == '__main__':
    # args = parser.parse_args()

    p2sql = Pheno2SQL(**config.get_pheno2sql_parameters())
    p2sql.load_data()
