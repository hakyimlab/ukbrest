FROM continuumio/miniconda3
MAINTAINER Milton Pividori <miltondp@gmail.com>

ENV UKBREST_GENOTYPE_PATH="/var/lib/genotype"
ENV UKBREST_PHENOTYPE_PATH="/var/lib/phenotype"

COPY ukbrest /opt/ukbrest
ENV PYTHONPATH="/opt"

COPY environment.yml /opt/
RUN conda env update -n root -f /opt/environment.yml

# Docker repository for PostgreSQL
RUN wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | \
    apt-key add - \
  && echo deb http://apt.postgresql.org/pub/repos/apt/ jessie-pgdg main > /etc/apt/sources.list.d/postgresql.list \
  && DEBIAN_FRONTEND=noninteractive \
    apt-get update && apt-get install -y --no-install-recommends \
      postgresql-client-9.6 \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /opt

COPY docker/start.py /opt/
ENV GUNICORN_CMD_ARGS="--log-file=- -w 4 -k eventlet -b 0.0.0.0:5000"

EXPOSE 5000

ENTRYPOINT ["/opt/start.py"]
CMD [""]
