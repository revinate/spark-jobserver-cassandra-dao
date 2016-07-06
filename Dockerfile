FROM revinate/spark-jobserver:0.6.2.mesos-0.28.1.spark-1.6.1

ADD docker/docker.conf /app/docker.conf
ADD build/libs/spark-jobserver-cassandra-dao-all.jar /app/spark-jobserver-cassandra-dao.jar
