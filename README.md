[![Build Status](https://travis-ci.org/hakyimlab/ukbrest.svg?branch=master)](https://travis-ci.org/hakyimlab/ukbrest)
[![Coverage Status](https://coveralls.io/repos/github/hakyimlab/ukbrest/badge.svg?branch=master)](https://coveralls.io/github/hakyimlab/ukbrest?branch=master)
[![Docker Automated build](https://img.shields.io/docker/automated/jrottenberg/ffmpeg.svg)](https://hub.docker.com/r/hakyimlab/ukbrest/)


# ukbREST

**Title:** ukbREST: efficient and streamlined data access for reproducible research of large biobanks

**Authors:** Milton Pividori and Hae Kyung Im

TODO: link to bioRxiv

*Im-Lab (http://hakyimlab.org/), Section of Genetic Medicine, Department of Medicine, The University of Chicago*

# Abstract
Large biobanks, such as UK Biobank with half a million participants, are changing the scale and availability of genotypic and phenotypic data for researchers to ask fundamental questions about the biology of health and disease. The breadth of the UK Biobank data is enabling discoveries at an unprecedented pace.
However, this size and complexity pose new challenges to investigators who need to keep the accruing data up to date, comply with potential consent changes, and efficiently and reproducibly extract subsets of the data to answer specific scientific questions.
Here we propose a tool called ukbREST designed for the UK Biobank study (easily
extensible to other biobanks), which allows authorized users to efficiently 
retrieve phenotypic and genetic data. It exposes a REST API that makes data highly accessible inside a private and secure network, allowing the data specification in a human readable text format easily shareable with other researchers.
These characteristics make ukbREST an important tool to make biobank’s valuable data more readily accessible to the research community but also facilitate reproducibility of the analysis, a key aspect of science.

# Architecture and setup overview
<p align="center">
<img src="/misc/ukbrest_arch.png" alt="Setup and architecture image" width="75%"/>
</p>

# Installation
You only need to install ukbREST in a server/computer; clients can connect to it and
make queries just using standard tools like `curl`. The quickest way to get ukbREST is to use
[our Docker image](https://hub.docker.com/r/hakyimlab/ukbrest/). So install
[Docker](https://docs.docker.com/) and follow the steps below. Just make sure, once
you installed Docker, that you have **enough disk space** (in macOS go to Preferences/Disk and increase the
value).

If you just want to give ukbREST a try, and you are not a UK Biobank user, you
can follow the [guide in the wiki](https://github.com/hakyimlab/ukbrest/wiki)
and use our simulated data.

## Step 1: Pre-process
If you are an approved UK Biobank researcher you are probably already familiar with this.
Once you downloaded your encrypted application files, decrypt them and convert them
to CSV and HTML formats using `ukbconv`. Checkout the
[Data Showcase documentation](http://biobank.ctsu.ox.ac.uk/crystal/).

Copy all CSV and HTML files to a particular folder (for example, called `phenotype`).
You will have one CSV and one HTML file per dataset, each one with a specific *Basket ID*, like
for example the ones shown below for four different datasets with Basket IDs 1111, 2222, 3333, 4444:
```
$ ls -lh phenotype/*
-rw-rw-r-- 1   6.6G Jul  2 23:22 phenotype/ukb1111.csv
-rw-rw-r-- 1   6.4M Jul  2 23:19 phenotype/ukb1111.html
-rw-rw-r-- 1   2.7G Jul  2 23:20 phenotype/ukb2222.csv
-rw-rw-r-- 1   4.5M Jul  2 23:19 phenotype/ukb2222.html
-rw-rw-r-- 1  1012M Jul  2 23:22 phenotype/ukb3333.csv
-rw-rw-r-- 1   192K Jul  2 23:19 phenotype/ukb3333.html
-rw-rw-r-- 1    22G Jul  2 23:24 phenotype/ukb4444.csv
-rw-rw-r-- 1   4.1M Jul  2 23:19 phenotype/ukb4444.html
```

Make sure your phenotype CSV files do not have overlapping data-fields (use the latest
data refresh for each basket).

For the genotype data you'll also have a specific folder, for instance, called `genotype`.
Here you have to copy your `bgen`, `bgi` (BGEN index files) and `sample` (BGEN sample) files:

```
$ ls -lh genotype/*
-rw-rw-r-- 1  114G Mar 16 09:51 genotype/ukb_imp_chr10_v3.bgen
-rw-rw-r-- 1  198M Mar 16 10:12 genotype/ukb_imp_chr10_v3.bgen.bgi
-rw-rw-r-- 1  109G Mar 16 09:52 genotype/ukb_imp_chr11_v3.bgen
-rw-rw-r-- 1  201M Mar 16 10:12 genotype/ukb_imp_chr11_v3.bgen.bgi
-rw-rw-r-- 1  109G Mar 16 09:54 genotype/ukb_imp_chr12_v3.bgen
[...]
-rw-rw-r-- 1  9.3M Apr  6 09:41 genotype/ukb12345_imp_chr1_v3_s487395.sample
```

## Step 2: Setup
Here we are going to start PostgreSQL and load the phenotype data into it.
Start Docker in your server/computer and pull the PostgreSQL and ukbREST images:

```
$ docker pull postgres:9.6
```

```
$ docker pull hakyimlab/ukbrest
```

Create a network in Docker that we'll use to connect ukbREST with PostgreSQL:

```
$ docker network create ukb
```

Start the PostgreSQL container (here we are using user `test` with password `test`; you should
choose a stronger one):

```
$ docker run -d --name pg --net ukb -p 127.0.0.1:5432:5432 \
  -e POSTGRES_USER=test -e POSTGRES_PASSWORD=test \
  -e POSTGRES_DB=ukb \
  postgres:9.6
```

Then use the ukbREST Docker image to load your phenotype data into the PostgreSQL database.
Here we are loading your CSV/HTML main datasets, but keep in mind that you can also load Sample-QC
or relatedness data, which is provided separately in UK Biobank. This is covered in
[the wiki](https://github.com/hakyimlab/ukbrest/wiki/Load-real-UK-Biobank-data).

In the command below, replace the bold text with the full path of both your phenotype and genotype folder,
as well as the right name of your `.sample` file.

<pre>
$ docker run --rm --net ukb \
  -v <b>/full/path/to/genotype/folder/</b>:/var/lib/genotype \
  -v <b>/full/path/to/phenotype/folder/</b>:/var/lib/phenotype \
  -e UKBREST_GENOTYPE_BGEN_SAMPLE_FILE="<b>ukb12345_imp_chr1_v3_s487395.sample</b>" \
  -e UKBREST_DB_URI="postgresql://test:test@pg:5432/ukb" \
  hakyimlab/ukbrest --load

[...]
2018-07-20 22:50:34,962 - ukbrest - INFO - Loading finished!
</pre>

Sometimes we found that the CSV file have a wrong encoding, making Python fail when reading
the file. If ukbREST found this, you'll see an error message about **Unicode decoding error**.
Check out [the documentation](https://github.com/hakyimlab/ukbrest/wiki/Load-real-UK-Biobank-data)
to know how to fix it.

COMPLETE: The documentation also explain the SQL scheme used, so you can take full advantage of it.

## Step 3: Start
Now you only need to start the ukbREST server:

<pre>
$ docker run --rm --net ukb -p 127.0.0.1:5000:5000 \
  -v /full/path/to/<b>genotype</b>/folder/:/var/lib/genotype \
  -e UKBREST_SQL_CHUNKSIZE="10000" \
  -e UKBREST_DB_URI="postgresql://test:test@pg:5432/ukb" \
  hakyimlab/ukbrest
</pre>

You don't need to specify the genotype folder (`-v /full/path/to/[...]:/var/lib/genotype`)
if you are not going to use genotype queries.

For **security reasons**, note that with these commands both the ukbREST server
and the PostgreSQL are only reachable from your own computer/server. No one from the
network will be able to make any queries other than you from the computer where
ukbREST is running.

Check out [the documentation](https://github.com/hakyimlab/ukbrest/wiki)
to setup ukbREST in a private and secure network and how to add **user authentication**
and **SSL encryption**.

## Step 4: Query
Once the ukbREST is up and running, you can request any data-field using
[different query methods](https://github.com/hakyimlab/ukbrest/wiki/Phenotype-queries).
Column names for data-fields have this format: `c{DATA_FIELD_ID}_{INSTANCE}_{ARRAY}`.

### Phenotype queries
ukbREST lets you make queries in different ways. If you only need to access some data-fields,
you can use standard tools like `curl` to make your query. You can also use a **YAML file** to write
your data specification in one place and easily share it (for instance, when submitting your manuscript),
improving reproducibility of results for others working on UK Biobank.

#### Using the command line
You can request a single or multiple data-fields using standard tools like `curl`:

Here we request two data-fields: 
* Data field ID 50 ([Standing height](http://biobank.ctsu.ox.ac.uk/showcase/field.cgi?id=50)),
instance 0 (Initial assessment visit 2006-2010), array 0 (this field is single-valued),
which has a column name `c50_0_0`. We rename this data-field to `height`.
* Data field ID 21002 ([Weight](http://biobank.ctsu.ox.ac.uk/showcase/field.cgi?id=21002)),
instance 2 (First repeat assessment visit 2012-13), array 0 (single-valued), which has a column
name `c21002_2_0`. We rename it to `weight`.
```
$ curl -G \
  -HAccept:text/csv \
  "http://127.0.0.1:5000/ukbrest/api/v1.0/phenotype" \
  --data-urlencode "columns=c50_0_0 as height" \
  --data-urlencode "columns=c21002_1_0 as weight" \
  > mydata.csv
```

Your data will be saved in file `mydata.csv`.

#### Using a YAML file

You can write your data specification in a YAML file. Take a look at this real example (we don't
show results, of course, but you can try it with your UK Biobank data):

```
$ cat my_query.yaml
samples_filters:
  - cin_white_british_ancestry_subset_0_0 = 1
  - eid not in (select eid from withdrawls)
  - eid > 0

data:
  sex: c31_0_0
  smoking_status: >
    coalesce(
      nullifneg(c20116_2_0), nullifneg(c20116_1_0), nullifneg(c20116_0_0)
    )
  asthma:
    case_control:
      20002:
        coding: 1111
      41202:
        coding: [J45, J450, J451, J458, J459]
  hypertension:
    sql:
      1: >
        eid in (
          select eid from events
          where field_id in (values(20002)) and event in (
            select * from get_children_codings('20002', array[1081])
          )
        )
      0: >
        eid not in (
          select eid from events
          where field_id in (values(20002)) and event in (
            select * from get_children_codings('20002', array[1081, 1085])
          )
        )

$ curl -X POST \
  -H "Accept: text/csv" \
  -F file=@my_query.yaml \
  -F section=data \
  http://127.0.0.1:5100/ukbrest/api/v1.0/query \
  > my_data.csv
```

The YAML file above has two sections: `sample_filters` which is a set of filters applied to all samples, and `data` which defines a data specification that will be translated to a CSV file later. You can have as many data specifications as you want. The `data` section has four columns:

* `sex`: it just select data field [31](http://biobank.ctsu.ox.ac.uk/showcase/field.cgi?id=31), instance 0, array 0.
* `smoking_status`: picks the first non empty value from all instances of
[data-field 20116](http://biobank.ctsu.ox.ac.uk/showcase/field.cgi?id=20116), giving priority to the
latest data from instance 2 to instance 0. Since this data-field has
[a coding](http://biobank.ctsu.ox.ac.uk/showcase/coding.cgi?id=90) that says that negative values are
those that `Prefer not to answer`, we consider these values as empty using the function `nullifneg` (null if negative).
* `asthma`: this one uses a feature for binary columns called `case_control`. Cases
(with value `1` for this column) will include all samples that have
self-reported asthma (data-field [20002](http://biobank.ctsu.ox.ac.uk/showcase/field.cgi?id=20002)
with value `1111`, which means asthma) *or* that have an ICD10 code (hospital
level data) that indicates asthma (`J45`, `J450`, `J451`, `J458`, `J459`). All the rest that don't meet
this criteria are controls (with value `0` for this column).
* `hypertension`: here we use a more advanced feature called `sql`, more better suited for real scenarios, and also employ another feature to select children of a hierarchically organized data-field (like self-reported diseases or ICD10 codes). First, with `sql`, you can specify a column with several categorical values: `1` and `0` in this case; for each of them you can write the SQL code with the conditions. The SQL code for category `1` will contain all samples that have self-reported (data-field [20002](http://biobank.ctsu.ox.ac.uk/showcase/field.cgi?id=20002))
any disease in the tree of cardiovascular/hypertension: this includes `hypertension`
itself but also `essential hypertension` and `gestational hypertension/pre-eclampsia`. For this you use the `get_children_codings` SQL function, indicating the data-field (20002) and the node id of the disease of interest (`1081` for hypertension; take a look at
[the codings for data-field 20002](http://biobank.ctsu.ox.ac.uk/showcase/coding.cgi?id=6)). In this case we are including all instances of data-field 20002. Something similar is done for category `0`, but in this case we are excluding (`eid not in...`) all individual with any disease under parents `hypertension` (node id `1081`) and `venous thromboembolic disease` (node id `1085`). Keep in mind that function `get_children_codings` works recursively, so *all* children down in the tree will be selected. If you would like to choose *all* individuals with *any* self-reported cardiovascular disease you would use `get_children_codings('20002', array[1071])`.


### Genotype queries
**TODO:** needs test.

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


# Documentation

Check out the [wiki pages](https://github.com/hakyimlab/ukbrest/wiki) for more information.
