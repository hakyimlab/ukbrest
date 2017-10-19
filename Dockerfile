FROM continuumio/miniconda3
MAINTAINER Milton Pividori <miltondp@gmail.com>

# Setup conda environment
COPY environment.yml /opt/
RUN conda env update -n root -f /opt/environment.yml \
  && conda clean --all

# Docker repository for PostgreSQL, install client programs
RUN wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | \
    apt-key add - \
  && echo deb http://apt.postgresql.org/pub/repos/apt/ jessie-pgdg main > /etc/apt/sources.list.d/postgresql.list \
  && DEBIAN_FRONTEND=noninteractive \
    apt-get update && apt-get install -y --no-install-recommends \
      postgresql-client-9.6 \
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
  && ./waf-1.8.13 configure \
  && ./waf-1.8.13 \
  && ./build/test/test_bgen \
  && mv build/apps/bgenix /usr/local/bin/ \
  && mv build/apps/cat-bgen /usr/local/bin/ \
  && mv build/apps/edit-bgen /usr/local/bin/ \
  && apt-get remove -y build-essential zlib1g-dev libbz2-dev mercurial \
  && apt-get autoremove -y \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

# Copy ukbrest code
ENV UKBREST_GENOTYPE_PATH="/var/lib/genotype"
ENV UKBREST_PHENOTYPE_PATH="/var/lib/phenotype"

COPY ukbrest /opt/ukbrest
ENV PYTHONPATH="/opt"

WORKDIR /opt

COPY docker/start.py /opt/
ENV WEB_CONCURRENCY=4
ENV GUNICORN_CMD_ARGS="--log-file=- -k eventlet --timeout 1000 -b 0.0.0.0:5000"

EXPOSE 5000

ENTRYPOINT ["/opt/start.py"]
CMD [""]
