# elastic-unit
Testing with an Embedded Elasticsearch. 

Use to test your application code against a functional, single node Elasticsearch cluster without starting or managing an extra process (VM or Container).

See the https://github.com/wdekker/elastic-unit/tree/master/src/test/java/dev/dekker/elasticunit for code samples on usage.

## Why would I use this instead of:

### Elastic's [Java Testing Framework](https://www.elastic.co/guide/en/elasticsearch/reference/current/testing-framework.html):
This project relies heavily on the Elastic's Testing Framework, but it wasn't usable as is for cases. For instance it is built on top of [Carrotsearch Randomized Testing](https://labs.carrotsearch.com/randomizedtesting.html) framework and this is very restrictive when using threads. Also plugins are not included so it is not possible to test all analyzers or non core api like reindex.

### Allegro's [Embedded Elasticsearch](https://github.com/allegro/embedded-elasticsearch)
An excellent framework which can be used for the same purpose and even supports multiple versions of Elasticsearch. The difference is that it will start ElasticSearch in a separate JVM which is just heavier on resources and slower then running it in the same VM as where the tests run.

### Docker or VMs
Using Docker images allows you to create a more production like environment and perhaps leads to more reliable results. However there isan overhead in managing and setting up these containers and will be more resource intensive.

Disclaimer: the cluster is started in the same JVM which is not supported by Elastic.
