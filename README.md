# spark-jobserver-cassandra-dao
Cassandra dao plugin for (spark jobserver)[https://github.com/spark-jobserver/spark-jobserver] to keep jobs and jars related data persisted into cassandra. 

This plugin depends on cassandra 3.0+ and it requires [cassandra lucence plugin](https://github.com/Stratio/cassandra-lucene-index) to get jobs sorted based on the start_time.

To build the project: ```./gradlew clean build shadowJ```

To run the test: ```./gradlew test```

To try it out with quickly with docker: ```./gradlew startSparkJobServerContainer```

To run it with standalone spark jobserver, you will need to:

- Update the config file for the cassandra dao specific config:
```
  jobserver {
    port = 8090
    jobdao = com.revinate.spark.jobserver.io.JobCassandraDAO

    context-per-jvm = false
    cassandradao {
      rootdir = /tmp/spark-job-server/sqldao/data # dir for caching the jar
      # user =
      # password =
      contactpoints: ${?CASSANDRA_CONTACT_POINTS}
      datacenter: datacenter1
      keyspace: jobserver_dao
    }
  }
```
take a look at the [docker config file](https://github.com/revinate/spark-jobserver-cassandra-dao/blob/master/docker/docker.conf) as an example of full config file.

- Update the start script with param: ```--jar spark-jobserver-cassandra-dao.jar``` to include the jar into the spark classpath of the spark job server.
