# Quarry

Quarry is an open source platform-as-a-service for Apache Spark. It allows you to run SQL and PySpark jobs using a simple HTTP API. You can easily boot and scale Spark Clusters and ingest data into Quarry by sending it events over HTTP. Quarry also has extensible database adapters to allow for the import and export of data from your operational databases. Currently, Quarry uses Amazon EC2 to launch clusters and Amazon S3 to store data, but we plan to support other cloud platforms and an on-premise solution in the future.

# Using Quarry

Quarry needs a few things to run:

* Zookeeper
* Redis
* RabbitMQ
* MySQL

Eventually, we'll take care of the setup of these services, but for now, you'll need to do it manually. Once you have those set up, you'll need to copy the file `conf.yml.template` to `conf.yml` and set your configuration options up properly. Once you've configured everything, you should be able to just type `./run.py` and then navigate to port 8000 on whatever host you're running Quarry on.
