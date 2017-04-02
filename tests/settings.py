
# PostgreSQL
# Recommended way to run PostgreSQL:
#   $ docker run --rm --name ukbrest_pg \
#       -e POSTGRES_USER=test -e POSTGRES_PASSWORD=test \
#       -e POSTGRES_DB=ukb -p 5432:5432 postgres:9.6
POSTGRESQL_ENGINE='postgresql://test:test@localhost:5432/ukb'

# SQLite
SQLITE_ENGINE='sqlite:///tmp.db'
