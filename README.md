[![Build Status](https://travis-ci.org/hakyimlab/ukbrest.svg?branch=master)](https://travis-ci.org/hakyimlab/ukbrest)
[![Coverage Status](https://coveralls.io/repos/github/hakyimlab/ukbrest/badge.svg?branch=master)](https://coveralls.io/github/hakyimlab/ukbrest?branch=master)
[![Docker Automated build](https://img.shields.io/badge/Docker%20build-automated-blue.svg)](https://hub.docker.com/r/hakyimlab/ukbrest/)


# ukbREST

**Title:** ukbREST: efficient and streamlined data access for reproducible research of large biobanks

**Authors:** Milton Pividori and Hae Kyung Im

**DOI:** https://doi.org/10.1093/bioinformatics/bty925

*Im-Lab (http://hakyimlab.org/), Section of Genetic Medicine, Department of Medicine, The University of Chicago.*

*Center for Translational Data Science (https://ctds.uchicago.edu/), The University of Chicago.*

**Join our mailing list here:** https://groups.google.com/d/forum/ukbrest

# Abstract
Large biobanks, such as UK Biobank with half a million participants, are changing the scale and availability of genotypic and phenotypic data for researchers to ask fundamental questions about the biology of health and disease. The breadth of the UK Biobank data is enabling discoveries at an unprecedented pace.
However, this size and complexity pose new challenges to investigators who need to keep the accruing data up to date, comply with potential consent changes, and efficiently and reproducibly extract subsets of the data to answer specific scientific questions.
Here we propose a tool called ukbREST designed for the UK Biobank study (easily
extensible to other biobanks), which allows authorized users to efficiently 
retrieve phenotypic and genetic data. It exposes a REST API that makes data highly accessible inside a private and secure network, allowing the data specification in a human readable text format easily shareable with other researchers.
These characteristics make ukbREST an important tool to make biobank’s valuable data more readily accessible to the research community and facilitate reproducibility of the analysis, a key aspect of science.

# Architecture and setup overview
<p align="center">
 <img src="https://github.com/hakyimlab/ukbrest/blob/master/misc/ukbrest_arch.png" alt="Setup and architecture image" width="75%"/>
</p>

# News
 * 2020-05-22: ukbREST supports [loading](https://github.com/hakyimlab/ukbrest/wiki/Load-real-UK-Biobank-data) and [querying](https://github.com/hakyimlab/ukbrest/wiki/Electronic-health-record-queries) electronic health records from the UK Biobank.
 * 2019-12-06: the installation steps for macOS and PostgreSQL have been updated. [Check it out!](https://github.com/hakyimlab/ukbrest/wiki/Installation-instructions)
 * 2018-11-25: fix when a dataset has a data-field already loaded. Docker image is now updated.
 Check out the [documentation](https://github.com/hakyimlab/ukbrest/wiki/Load-real-UK-Biobank-data) (Section `Duplicated data-fields`).

# Installation
You only need to install ukbREST in a server/computer; clients can connect to it and
make queries just using standard tools like `curl`. The quickest way to get ukbREST is to use
[our Docker image](https://hub.docker.com/r/hakyimlab/ukbrest/). So install
[Docker](https://docs.docker.com/) and follow the steps below. Just make sure, once
you installed Docker, that you have **enough disk space** (in macOS go to Preferences/Disk and increase the
value). Take a look a the wiki to know the general specifications expected for a computer/server.

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
$ docker pull postgres:11
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
  postgres:11
```

Keep in mind that the above command runs PostgreSQL with the default settings. That could make it work **really slow** when
you send a query to ukbREST. See the installation instructions in the
[wiki](https://github.com/hakyimlab/ukbrest/wiki) for more details.

Then use the ukbREST Docker image to load your phenotype data into the PostgreSQL database.
Here we are only loading your CSV/HTML main datasets, but keep in mind that you can also load **Sample-QC**
or **relatedness data**, which is provided separately in UK Biobank. This is covered in
[the wiki](https://github.com/hakyimlab/ukbrest/wiki/Load-real-UK-Biobank-data).

In the command below, replace the bold text with the full path of both your phenotype and genotype folder,
as well as the right name of your `.sample` file.

<pre>
$ docker run --rm --net ukb \
  -v <b>/full/path/to/genotype/folder/</b>:/var/lib/genotype \
  -v <b>/full/path/to/phenotype/folder/</b>:/var/lib/phenotype \
  -e UKBREST_GENOTYPE_BGEN_SAMPLE_FILE="<b>ukb12345_imp_chr1_v3_s487395.sample</b>" \
  -e UKBREST_DB_URI="postgresql://test:test@pg:5432/ukb" \
  -e UKBREST_LOADING_N_JOBS=2 \
  hakyimlab/ukbrest --load

[...]
2018-07-20 22:50:34,962 - ukbrest - INFO - Loading finished!
</pre>

Sometimes we found that the CSV file have a wrong encoding, making Python fail when reading
the file. If ukbREST found this, you'll see an error message about **Unicode decoding error**.
Check out [the documentation](https://github.com/hakyimlab/ukbrest/wiki/Load-real-UK-Biobank-data)
to know how to fix it.

You can also adjust the number of cores used when loading the data with the
variable `UKBREST_LOADING_N_JOBS` (set to 2 cores in the example above).

The documentation also explain the [SQL schema](https://github.com/hakyimlab/ukbrest/wiki/SQL-schema),
so you can take full advantage of it.

Once your main datasets are loaded, you only need to complete two more steps: 1) load the data-field codings and
2) some useful SQL functions. You do this by just running two commands.

To load the data-field codings, run this:
```
$ docker run --rm --net ukb \
  -e UKBREST_DB_URI="postgresql://test:test@pg:5432/ukb" \
  hakyimlab/ukbrest --load-codings
```
This will load most of the data-field codings from the UK Biobank Data Showcase (they are in `.tsv` format in
the [codings folder](https://github.com/hakyimlab/ukbrest/tree/master/misc/codings)). This includes, for instance,
[data coding 19](http://biobank.ctsu.ox.ac.uk/showcase/coding.cgi?id=19), which is used for
[data-field 41202](http://biobank.ctsu.ox.ac.uk/showcase/field.cgi?id=41202)
(Diagnoses - main ICD10).
For your application, however, you could need to download a few more if you have specific data-fields.
This is covered in [the documentation](https://github.com/hakyimlab/ukbrest/wiki/Load-real-UK-Biobank-data).

Finally, run this command to create some useful SQL functions you will likely use in your queries:
```
$ docker run --rm --net ukb \
  -e UKBREST_DB_URI="postgresql://test:test@pg:5432/ukb" \
  hakyimlab/ukbrest --load-sql
```

## Step 3: Start
Now you only need to start the ukbREST server:

<pre>
$ docker run --rm --net ukb -p 127.0.0.1:5000:5000 \
  -e UKBREST_SQL_CHUNKSIZE="10000" \
  -e UKBREST_DB_URI="postgresql://test:test@pg:5432/ukb" \
  hakyimlab/ukbrest
</pre>

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
improving reproducibility of results for others working on UK Biobank. You can also specify the output file format (for example, CSV or the format used by plink or BGENIE).

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
  > my_data.csv
```

Your data will be saved in file `my_data.csv`.

#### Using a YAML file

You can write your data specification in a YAML file. Take a look at this real example (we don't
show results, of course, but you can try it with your UK Biobank data):

```
$ cat my_query.yaml
samples_filters:
  - c22006_0_0 = '1'
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
  http://127.0.0.1:5000/ukbrest/api/v1.0/query \
  > my_data.csv
```

The YAML file above has two sections: `samples_filters` which is a set of filters applied to all samples
(in the example above we are considering Caucasian specified in data-field 22006), and `data` which defines
a data specification that will be translated to a CSV file later. You can have as many data
specifications in one file as you want (you choose the one you want when calling `curl`). The `samples_filters` will be applied on all of them.

The `data` section has four columns:

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
* `hypertension`: here we use a more advanced feature called `sql`, better suited for complex real scenarios, and also employ another feature to select children of a hierarchically organized data-field (like self-reported diseases or ICD10 codes). First, with `sql`, you can specify a column with several categorical values: `1` and `0` in this case; for each of them you can write the SQL code with the conditions. The SQL code for category `1` will contain all samples that have self-reported (data-field [20002](http://biobank.ctsu.ox.ac.uk/showcase/field.cgi?id=20002))
any disease in the tree of cardiovascular/hypertension: this includes `hypertension`
itself but also `essential hypertension` and `gestational hypertension/pre-eclampsia`. For this you use the `get_children_codings` SQL function, indicating the data-field (20002) and the node id of the disease of interest (`1081` for hypertension; take a look at
[the codings for data-field 20002](http://biobank.ctsu.ox.ac.uk/showcase/coding.cgi?id=6)). In this case we are including all instances of data-field 20002. Something similar is done for category `0`, but in this case we are excluding (`eid not in...`) all individuals with any disease under parents `hypertension` (node id `1081`) and `venous thromboembolic disease` (node id `1085`). Keep in mind that function `get_children_codings` works recursively, so *all* children down in the tree will be selected. If you would like, for example, to choose *all* individuals with *any* self-reported cardiovascular disease you would use `get_children_codings('20002', array[1071])`.


The [wiki](https://github.com/hakyimlab/ukbrest/wiki) contains a page with real examples of YAML files. We encourage you to share yours!

### Genotype queries

When you started ukbREST before, you didn't specified the genotype directory. This is fine if you are planning
to just query data-fields. If you do want to get BGEN subsets, you need to add two parameters
when staring ukbREST:

<pre>
$ docker run --rm --net ukb -p 127.0.0.1:5000:5000 \
  -v <b>/full/path/to/genotype/folder/</b>:/var/lib/genotype \
  -e <b>UKBREST_GENOTYPE_BGEN_FILE_NAMING="ukb_imp_chr{:d}_v3.bgen"</b> \
  -e UKBREST_SQL_CHUNKSIZE="10000" \
  -e UKBREST_DB_URI="postgresql://test:test@pg:5432/ukb" \
  hakyimlab/ukbrest
</pre>

Look at the bold text above. You need to put your full path to the genotype folder (where both the `bgen` and
`bgi` index files reside), and also specify the `bgen` file name template with the environmental variable
`UKBREST_GENOTYPE_BGEN_FILE_NAMING`. The substring `{:d}` will be replaced by the chromosome number.

So if you want to get a subset of the chromosome 22, let's say position from 0 to 1000, you run
something like this:

```bash
$ curl http://localhost:5000/ukbrest/api/v1.0/genotype/22/positions/0/1000 \
  > chr22_subset.bgen
```

With the query below, you can get a subset of the BGEN using a file specifying rsids:

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

$ curl -X POST \
  -F file=@rsids.txt \
  http://localhost:5000/ukbrest/api/v1.0/genotype/1/rsids \
  > chr1_subset.bgen
```

Note that in these two examples you get a `bgen` (binary) file. If you want to read it from your scripts in Python,
for instance, you can use a package like this one: https://github.com/limix/bgen-reader-py


# Documentation

Check out the [wiki pages](https://github.com/hakyimlab/ukbrest/wiki) for more information.
