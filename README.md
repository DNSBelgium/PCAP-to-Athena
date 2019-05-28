PCAP to Athena
==============

The goal of this project is to convert PCAP files into Parquet format and make them available via Amazon Athena.
The project uses libraries from Entrada and also contains code which was copied from Entrada and modified where needed.

Datamodel
=========

For the columns used in the parquet files, see http://entrada.sidnlabs.nl/docs/concepts/data_model/#dns

Configuration
=============

The software will automatically look for `application.properties` file in the user directory, where the java command is launched.
You can copy the `application.properties` from the source and adapt it to your environment.

Setting up
==========

Maxmind Geo IP
--------------

In order to enrich parquet file with information such as Country and ASN, the software looks up the IP in the Maxmind database.
This database is just a file that needs to be downloaded and made available to the software via configuration.

The default configuration locates the maxmind database in `${user.dir}/maxmind`.
To download the Maxmind database, run
```bash
cd maxmind && ./download_maxmind_geo_ip_db.sh
```

Third party libraries
---------------------

Athena JDBC driver is needed in order to connect to Amazon Athena and execute SQL statements.
To download the jar, run
```bash
cd lib && ./download_libs.sh
```

The script also download pcaplib4java and dnslib4java 

AWS S3
------

This software uses S3 buckets. The responsability of creation of the bucket is left to the reader.
Once bucket name(s) is (are) defined, the configuration in `application.properties` must be updated, in particular `pcap.bucket.name`, `pcap.archive.bucket.name`, `parquet.bucket.name`.

AWS Athena
----------

A database and a table must be created in AWS Athena and should be filled in the config at `athena.database.name` and `athena.table.name`.
Table can be created via the SQL statement in `src/resources/sql/athena-create-table.sql`.

AWS Credentials
---------------

You need a credentials file: `~/.aws/credentials` with a `[default]` profile containing a valid pair of aws_access_key_id and aws_secret_access_key.
Be sure to have the correct permissions for S3 and Athena.

Build
=====

The project uses Maven for building sources. Once Maxmind DB and third party libraries are downloaded, you can run
```bash
./mvnw validate package
```
to install jar in your local maven repository, and build and package sources to a jar. The jar will be located in `target/`.

Tests
=====

The test folder contains both unit tests and integration tests.
The unit tests can be runned by `./mvnw test`.

Some tests require access to AWS and are skipped by default.
Use `./run-integration-tests.sh` to execute these tests.

To run these tests, you need a credentials file: `~/.aws/credentials`
with a `[default]` profile containing a valid pair of aws_access_key_id and aws_secret_access_key

The IAM user corresponding to these access key should have write permissions in the S3 buckets used in the tests.
See `src/test/resources/test-application.properties` for the names of the S3 buckets

Attribution
============

This product includes ENTRADA created by <a href="https://www.sidnlabs.nl">SIDN Labs</a>, available from
<a href="http://entrada.sidnlabs.nl">http://entrada.sidnlabs.nl</a>.
