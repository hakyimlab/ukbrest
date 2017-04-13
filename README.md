[![Build Status](https://travis-ci.org/miltondp/ukbrest.svg?branch=master)](https://travis-ci.org/miltondp/ukbrest)
[![Coverage Status](https://coveralls.io/repos/github/miltondp/ukbrest/badge.svg?branch=master)](https://coveralls.io/github/miltondp/ukbrest?branch=master)

# ukbrest

## Downloading and running
The easiest way to get ukbrest up and running is using Docker. The instructions below are just to get things working
quickly. You should read the Docker documentation if you want to, for example, keep data saved across different runs.

The first thing you have to do is creating a virtual network and getting PostgreSQL up:

```bash
$ docker network create ukb

$ docker run --rm --name pg --net ukb -e POSTGRES_USER=test \
  -e POSTGRES_PASSWORD=test -e POSTGRES_DB=ukb -p 5432:5432 \
  postgres:9.6
```

Then you have to load your phenotype data into PostgreSQL:

```bash
$ docker run --rm --net ukb \
  -v /mnt/phenotype/:/var/lib/phenotype \
  -e UKBREST_DB_URI="postgresql://test:test@pg:5432/ukb" \
  miltondp/ukbrest --load
```

The third step consists in indexing your genotype data. You have to use the
[bgenix](https://bitbucket.org/gavinband/bgen/wiki/bgenix) indexer to generate a `.bgi` file
for each `.bgen` file (for each chromosome). This feature will be added soon to ukbrest, so you don't have to
download and compile bgenix.

Finally, run ukbrest with:

```bash
$ docker run --rm --net ukb -p 5000:5000 \
  -v /mnt/genotype:/var/lib/genotype \
  -e UKBREST_DB_URI="postgresql://test:test@pg:5432/ukb" \
  miltondp/ukbrest
```

Make sure the directory `/mnt/genotype` (you can choose another one) has you genotype data (chr21impv1.bgen,
chr21impv1.bgen.bgi, etc).


## Querying genotype data

Query chromosome 1, positions from 0 to 1000:
```bash
$ curl -HAccept:application/octel-stream \
  "http://localhost:5000/ukbrest/api/v1.0/genotype/1/positions/0/1000" \
  > test.bgen
```

Query by chromosome and a file specifying rsids:
```bash
$ cat rsids.txt
rs367896724
rs540431307
rs555500075
rs548419688
rs568405545
rs534229142
rs537182016
rs376342519
rs558604819

$ curl -HAccept:application/octel-stream \
-X POST -F file=@rsids.txt \
"http://localhost:5000/ukbrest/api/v1.0/genotype/1/rsids" \
> test2.bgen
```


## Querying phenotype data

CSV format:
```bash
$ curl -HAccept:text/csv \
"http://localhost:5000/ukbrest/api/v1.0/phenotype\
?columns=c21_0_0\
&columns=c34_0_0\
&filters=eid <1000100"
```

Phenotype format (used by plink2, for example):
```bash
$ curl -HAccept:text/phenotype \
"http://localhost:5000/ukbrest/api/v1.0/phenotype\
?columns=c21_0_0\
&columns=c34_0_0\
&filters=eid <1000100"
```
