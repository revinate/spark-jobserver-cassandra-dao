package com.revinate.spark.jobserver.io

import java.io.{BufferedOutputStream, File, FileOutputStream}
import java.nio.ByteBuffer
import java.util.UUID

import com.datastax.driver.core._
import com.datastax.driver.core.policies.{DCAwareRoundRobinPolicy, TokenAwarePolicy}
import com.datastax.driver.core.querybuilder.QueryBuilder
import com.datastax.driver.core.querybuilder.QueryBuilder._
import com.datastax.driver.core.utils.UUIDs
import com.stratio.cassandra.lucene.builder.Builder._
import com.stratio.cassandra.lucene.builder.search.sort.SimpleSortField
import com.typesafe.config.{Config, ConfigFactory, ConfigRenderOptions}
import org.joda.time.DateTime
import org.slf4j.LoggerFactory
import spark.jobserver.io.{JarInfo, JobDAO, JobInfo}

import scala.collection.JavaConversions._

class JobCassandraDAO(config: Config) extends JobDAO {
  val TABLE_JARS = "jars"
  val TABLE_JAR_ID_LOOKUP = "jar_id_lookup"
  val TABLE_JOBS = "jobs"
  val TABLE_JOB_CONFIGS = "job_configs"

  private val rootDir = config.getString("spark.jobserver.cassandradao.rootdir")
  private val rootDirFile = new File(rootDir)
  private val logger = LoggerFactory.getLogger(getClass)

  // DB initialization
  val cassandraContactPoints = config.getString("spark.jobserver.cassandradao.contactpoints")
  val cassandraPort = try {
    config.getInt("spark.jobserver.cassandradao.port")
  } catch {
    case _: Throwable => 9042
  }
  val cassandraUser = try {
    config.getString("spark.jobserver.cassandradao.user")
  } catch {
    case _: Throwable => ""
  }
  val cassandraDataCenter = try {
    config.getString("spark.jobserver.cassandradao.datacenter")
  } catch {
    case _: Throwable => "datacenter1"
  }
  val cassandraPassword = try {
    config.getString("spark.jobserver.cassandradao.password")
  } catch {
    case _: Throwable => ""
  }
  val cassandraKeyspace = try {
    config.getString("spark.jobserver.cassandradao.keyspace")
  } catch {
    case _: Throwable => "jobserver_dao"
  }

  private val cassandraQueryOptions = new QueryOptions().setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM)
  private val cluster = Cluster.builder.addContactPoints(cassandraContactPoints.split(","): _*).withPort(cassandraPort)
    .withPort(cassandraPort)
    .withLoadBalancingPolicy(new TokenAwarePolicy(DCAwareRoundRobinPolicy.builder.withLocalDc(cassandraDataCenter).build))
    .withQueryOptions(cassandraQueryOptions)
    .withCredentials(cassandraUser, cassandraPassword)
    .build
  val cqlSession = cluster.init.connect(cassandraKeyspace)

  case class Jar(
                  jarId: UUID,
                  appName: String,
                  uploadTime: DateTime,
                  jar: ByteBuffer
                )

  object Jar {
    def apply(row: Row): Jar =
      Jar(jarId = row.getUUID("jar_id"),
        appName = row.getString("app_name"),
        uploadTime = Option(row.getTimestamp("upload_time")).map(new DateTime(_)).orNull,
        jar = row match {
          case r if r.getColumnDefinitions.contains("jar") => r.getBytes("jar")
          case _ => ByteBuffer.wrap(Array.emptyByteArray)
        }
      )
  }

  case class Job(
                  jobId: String,
                  contextName: String,
                  jarId: UUID,
                  classPath: String,
                  startTime: DateTime,
                  endTime: DateTime,
                  error: String
                ) {
  }

  object Job {
    def apply(row: Row): Job =
      Job(jobId = row.getString("job_id"),
        contextName = row.getString("context_name"),
        jarId = row.getUUID("jar_id"),
        classPath = row.getString("class_path"),
        startTime = Option(row.getTimestamp("start_time")).map(new DateTime(_)).orNull,
        endTime = Option(row.getTimestamp("end_time")).map(new DateTime(_)).orNull,
        error = row.getString("error"))
  }

  case class JobConfig(
                        jobId: String,
                        jobConfig: String
                      )


  // Server initialization
  init()

  private def init() {
    // Create the data directory if it doesn't exist
    if (!rootDirFile.exists()) {
      if (!rootDirFile.mkdirs()) {
        throw new RuntimeException("Could not create directory " + rootDir)
      }
    }

    setupSchemas()
  }

  override def saveJar(appName: String, uploadTime: DateTime, jarBytes: Array[Byte]) {
    cacheJar(appName, uploadTime, jarBytes)

    val jarId = UUIDs.timeBased
    cqlSession.execute(QueryBuilder.insertInto(TABLE_JARS)
      .value("jar_id", jarId)
      .value("app_name", appName)
      .value("upload_time", uploadTime.toDate)
      .value("jar", ByteBuffer.wrap(jarBytes)))

    cqlSession.execute(QueryBuilder.insertInto(TABLE_JAR_ID_LOOKUP)
      .value("app_name", appName)
      .value("upload_time", uploadTime.toDate)
      .value("jar_id", jarId))
  }

  override def getApps: Map[String, DateTime] =
    cqlSession.execute(select("jar_id", "app_name", "upload_time").from(TABLE_JARS)).all()
      .toList
      .map(Jar.apply)
      .map(jar => (jar.appName, jar.uploadTime))
      .groupBy(_._2).values
      .map(getLatestApp)
      .toMap

  override def saveJobConfig(jobId: String, jobConfig: Config) {
    cqlSession.execute(insertInto(TABLE_JOB_CONFIGS)
      .value("job_id", jobId)
      .value("job_config", jobConfig.root().render(ConfigRenderOptions.concise())))
  }

  override def getJobInfos(limit: Int): Seq[JobInfo] =
    cqlSession.execute(s"SELECT * from $TABLE_JOBS WHERE expr(jobs_index, ?) LIMIT $limit",
      search().refresh(true).sort(new SimpleSortField("start_time").reverse(true)).build())
      .toStream
      .map(Job.apply(_))
      .map(job => (job, cqlSession.executeAsync(select("jar_id", "app_name", "upload_time").from(TABLE_JARS).where(QueryBuilder.eq("jar_id", job.jarId)))))
      .map { case (job, future) =>
        val jarInfo = Option(future.get().one())
          .map(Jar.apply)
          .map(jar => JarInfo(jar.appName, jar.uploadTime))
          .orNull

        JobInfo(job.jobId,
          job.contextName,
          jarInfo,
          job.classPath,
          job.startTime,
          Option(job.endTime),
          Option(job.error).map(new Throwable(_))
        )
      }

  override def retrieveJarFile(appName: String, uploadTime: DateTime): String = {
    val jarFile = new File(rootDir, createJarName(appName, uploadTime) + ".jar")
    if (!jarFile.exists()) fetchAndCacheJarFile(appName, uploadTime)
    jarFile.getAbsolutePath
  }

  override def getJobInfo(jobId: String): Option[JobInfo] = {
    val row = Option(cqlSession.execute(select.from(TABLE_JOBS).where(QueryBuilder.eq("job_id", jobId)).limit(1)).one())

    row.map(Job.apply)
      .map(job =>
        JobInfo(job.jobId,
          job.contextName,
          getJarInfo(job.jarId).orNull,
          job.classPath,
          job.startTime,
          Option(job.endTime),
          Option(job.error).map(new Throwable(_))
        )
      )
  }

  override def saveJobInfo(jobInfo: JobInfo) {
    val jar = getJar(jobInfo.jarInfo.appName, jobInfo.jarInfo.uploadTime)

    Job(jobInfo.jobId, jobInfo.contextName, jar.map(_.jarId).orNull, jobInfo.classPath, jobInfo.startTime,
      jobInfo.endTime.orNull, jobInfo.error.map(_.getMessage).orNull)

    cqlSession.execute(insertInto(TABLE_JOBS).value("job_id", jobInfo.jobId)
      .value("context_name", jobInfo.contextName)
      .value("jar_id", jar.map(_.jarId).orNull)
      .value("class_path", jobInfo.classPath)
      .value("start_time", jobInfo.startTime.toDate)
      .value("end_time", jobInfo.endTime.map(_.toDate).orNull)
      .value("error", jobInfo.error.map(_.getMessage).orNull)
    )
  }

  override def getJobConfigs: Map[String, Config] =
    cqlSession.execute(QueryBuilder.select.from(TABLE_JOB_CONFIGS)).all()
      .toStream
      .map(row => (row.getString("job_id"), ConfigFactory.parseString(row.getString("job_config"))))
      .toMap

  // Cache the jar file into local file system.
  private def cacheJar(appName: String, uploadTime: DateTime, jarBytes: Array[Byte]) {
    val outFile = new File(rootDir, createJarName(appName, uploadTime) + ".jar")
    val bos = new BufferedOutputStream(new FileOutputStream(outFile))
    try {
      logger.debug("Writing {} bytes to file {}", jarBytes.size, outFile.getPath)
      bos.write(jarBytes)
      bos.flush()
    } finally {
      bos.close()
    }
  }

  // Create the name of the jar
  private def createJarName(appName: String, uploadTime: DateTime): String = appName + "-" + uploadTime

  // Get the latest app (which has the greatest uploadTime from a list of appname&uploadtime
  private def getLatestApp(group: List[(String, DateTime)]): (String, DateTime) =
    group.sortWith((t1, t2) => (t1._2 compareTo t2._2) == -1).head

  // Fetch the jar file from database and cache it into local file system.
  private def fetchAndCacheJarFile(appName: String, uploadTime: DateTime) {
    val jar = getJar(appName, uploadTime)
    cacheJar(appName, uploadTime, jar.map(_.jar.array).getOrElse(Array.empty[Byte]))
  }

  // Fetch the jar from the database
  private def getJar(appName: String, uploadTime: DateTime): Option[Jar] = {
    val row = cqlSession.execute(select("jar_id").from(TABLE_JAR_ID_LOOKUP)
      .where(QueryBuilder.eq("app_name", appName))
      .and(QueryBuilder.eq("upload_time", uploadTime.toDate)))
      .one

    logger.info("Fetch jar with jar id: {}", Option(row).map(_.getUUID("jar_id")))
    Option(row).map(_.getUUID("jar_id"))
      .map(getJar)
      .map(_.get)
  }

  // Find Jar by jar id
  private def getJar(jarId: UUID): Option[Jar] = {
    require(jarId != null, "Trying to fetch jar with a null jarId")
    val row = cqlSession.execute(select.from(TABLE_JARS).where(QueryBuilder.eq("jar_id", jarId)))

    Option(row.one).map(Jar.apply)
  }

  // Get the jar info by jar id
  private def getJarInfo(jarId: UUID): Option[JarInfo] = {
    val row = cqlSession.execute(select("jar_id", "app_name", "upload_time").from(TABLE_JARS).where(QueryBuilder.eq("jar_id", jarId)).limit(1)).one()

    Option(row).map(Jar.apply)
      .map(jar => JarInfo(jar.appName, jar.uploadTime))
  }

  // Setup the schemas
  private def setupSchemas() {
    cqlSession.execute(
      s"""
        |CREATE TABLE IF NOT EXISTS $TABLE_JARS (
        |    jar_id timeuuid,
        |    app_name text,
        |    upload_time timestamp,
        |    jar blob,
        |    PRIMARY KEY (jar_id)
        |);
      """.stripMargin)
    cqlSession.execute(
      s"""
        |CREATE TABLE IF NOT EXISTS $TABLE_JOBS (
        |    job_id text,
        |    context_name text,
        |    jar_id timeuuid,
        |    class_path text,
        |    start_time timestamp,
        |    end_time timestamp,
        |    error text,
        |    PRIMARY KEY (job_id)
        |)
      """.stripMargin)
    cqlSession.execute(
      s"""
        |CREATE TABLE IF NOT EXISTS $TABLE_JOB_CONFIGS (
        |    job_id text,
        |    job_config text,
        |    PRIMARY KEY (job_id)
        |)
      """.stripMargin)

    cqlSession.execute(
      s"""
        |CREATE TABLE IF NOT EXISTS $TABLE_JAR_ID_LOOKUP (
        |    app_name text,
        |    upload_time timestamp,
        |    jar_id timeuuid,
        |    PRIMARY KEY (app_name, upload_time)
        |)
      """.stripMargin)

    cqlSession.execute(
      s"""
        |CREATE CUSTOM INDEX IF NOT EXISTS jobs_index ON $TABLE_JOBS ()
        |USING 'com.stratio.cassandra.lucene.Index'
        |WITH OPTIONS = {
        |  'refresh_seconds' : '1',
        |  'schema' : '{
        |    fields : {
        |        start_time: {
        |            type      : "date",
        |            validated : true,
        |            pattern   : "yyyy-MM-dd HH:mm:ss",
        |            sorted    : true
        |       }
        |    }
        |  }'
        |};
      """.stripMargin
    )
  }
}
