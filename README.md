# elastic-unit
Testing with an Embedded Elasticsearch. 

Use to test your application code against a functional, single node Elasticsearch cluster without starting or managing an extra process (VM or Container).

See the https://github.com/wdekker/elastic-unit/tree/master/src/test/java/dev/dekker/elasticunit for code samples on usage.

Disclaimer: the cluster is started in the same JVM which is not supported by Elastic and may lead to unexpected results.
