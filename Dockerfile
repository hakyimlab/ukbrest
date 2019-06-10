FROM continuumio/miniconda3
MAINTAINER Milton Pividori <miltondp@gmail.com>

# Setup conda environment
COPY environment.yml /opt/
RUN conda env update -n root -f /opt/environment.yml \
  && conda clean --all

# Docker repository for PostgreSQL, install client programs
# IMPORTANT: although it is automated with lsb_release, debian version should match with continuumio/miniconda3
RUN DEBIAN_FRONTEND=noninteractive \
  apt-get update && apt-get install -y --no-install-recommends gnupg lsb-release \
  && wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add - \
  && echo "deb http://apt.postgresql.org/pub/repos/apt/ $(lsb_release -c -s)-pgdg main" >> /etc/apt/sources.list.d/pgdg.list \
  && apt-get update && apt-get install -y --no-install-recommends \
      postgresql-client-11 \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

# Compile and install bgenix
RUN export DEBIAN_FRONTEND=noninteractive \
  && apt-get update && apt-get install -y --no-install-recommends \
    build-essential zlib1g-dev libbz2-dev mercurial \
  && cd /tmp \
  && hg clone https://gavinband@bitbucket.org/gavinband/bgen -u master \
  && cd /tmp/bgen \
  && export PATH="/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin" \
  && ./waf configure --prefix=/usr/local \
  && ./waf \
  && ./build/test/unit/test_bgen \
  && ./waf install \
  && apt-get remove -y build-essential zlib1g-dev libbz2-dev mercurial \
  && apt-get autoremove -y \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

# Genotype and phenotype default directories
ENV UKBREST_GENOTYPE_PATH="/var/lib/genotype"
ENV UKBREST_PHENOTYPE_PATH="/var/lib/phenotype"

# Copy ukbrest code
COPY ukbrest /opt/ukbrest
ENV PYTHONPATH="/opt"
COPY utils /opt/utils

# Copy data codings
ENV UKBREST_CODINGS_PATH="/var/lib/codings"
COPY misc/codings /var/lib/codings

# Other environmental variables
ENV UKBREST_WITHDRAWALS_PATH="/var/lib/withdrawals"

WORKDIR /opt

COPY docker/start.py /opt/

# Gunicorn default settings
ENV GUNICORN_NUM_WORKERS="4"
ENV GUNICORN_BIND_ADDRESS="0.0.0.0:5000"
ENV GUNICORN_TIMEOUT="10000"
ENV GUNICORN_WORKER_CLASS="eventlet"
ENV GUNICORN_LOG_FILE="-"
ENV GUNICORN_EXTRA_ARGS=""

ENV GUNICORN_CMD_ARGS="--log-file=${GUNICORN_LOG_FILE} -k ${GUNICORN_WORKER_CLASS} -w ${GUNICORN_NUM_WORKERS} --timeout ${GUNICORN_TIMEOUT} -b ${GUNICORN_BIND_ADDRESS} ${GUNICORN_EXTRA_ARGS}"

EXPOSE 5000

ENTRYPOINT ["/opt/start.py"]
CMD [""]
