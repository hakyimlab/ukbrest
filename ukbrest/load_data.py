import argparse

from ukbrest.common.pheno2sql import Pheno2SQL
from ukbrest.common.postloader import Postloader
from ukbrest import config

parser = argparse.ArgumentParser()
parser.add_argument('--load-codings', dest='load_db', action='store_true')


if __name__ == '__main__':
    args = parser.parse_args()

    if args.load_codings:
        pl = Postloader(**config.get_postloader_parameters())
        pl.load_codings(**config.get_postloader_codings_parameters())
    else:
        p2sql = Pheno2SQL(**config.get_pheno2sql_parameters())
        p2sql.load_data(**config.get_pheno2sql_load_parameters())
