import java.io.{ FileReader, FileInputStream }
import java.util.Properties
import java.util.jar.Manifest

import aws.emr.EmrClient
import aws.model.MicroCluster
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceAsyncClient
import sbt._
import Keys._
import aws._

import scala.concurrent.duration._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{ Future, Await }

object EmrScaldingPlugin extends Plugin {

  override lazy val settings = Seq(commands += deployScaldingClass)

  lazy val deployScaldingClass =
    Command.command("runEmrTask") { (state: State) =>
      val newState = Command.process("assembly", state)

      // TODO get path to the assembly target
      val jar = new File(Project.current(newState).build.getPath + "/target/scala-2.10/" + Project.current(newState).project + ".jar")

      //TODO FIX Hadoop issue with slim. val jar = makeJar(state)

      val s3Jar = deployJar(state, jar)

      val task = s3Jar.map(jar =>
        lunchEmrTask(state, jar))

      Await.result(task, 5 minutes) // TODO transform in an async sbt task?
      state
    }

  private def lunchEmrTask(state: State, s3Jar: String) {
    val awsClient = new AmazonElasticMapReduceAsyncClient(credentials(state))
    val emrClient = EmrClient(bucketName, awsClient)

    val cluster = emrClient.createCluster(MicroCluster)

    val idStep = cluster.flatMap { c =>
      state.log.log(Level.Info, s"Cluster created, id: ${c.clusterId}")
      c.addJob(s3Jar, taskName)
    }

    val task = Await.result(idStep, 5 minutes)
    state.log.log(Level.Info, s"Task created: ${task.stepId}")
  }

  private def deployJar(state: State, jar: File) = {

    val s3Client = S3Client(bucketName, credentials(state))
    state.log.log(Level.Info, s"Deploying to S3: $jar")

    val s3Name = s3Client.deploy(jar)

    s3Name.foreach { s3File =>
      state.log.log(Level.Info, s"Deployed: $s3File ")
    }
    s3Name
  }

  private def collectClasses(classesDir: File): List[File] = {
    classesDir.listFiles.foldLeft(List.empty[File]) {
      case (currentList, file) if file.isFile => file :: currentList
      case (currentList, file) if file.isDirectory => collectClasses(file) ::: currentList
    }
  }

  private def credentials(state: State) = {
    val f = new File("aws.credentials")
    if (!f.exists()) {
      state.log.log(Level.Error, s"Unable to load $f")
    } else {
      state.log.log(Level.Error, s"Using credentials in $f")

      state.log.log(Level.Info, s"Bucket name: $bucketName")
      state.log.log(Level.Info, s"Task name: $taskName")
    }
    import com.amazonaws.auth.PropertiesCredentials
    new PropertiesCredentials(f)
  }

  val bucketName = properties.getProperty("bucketName")

  val taskName = properties.getProperty("taskName")

  lazy val properties = {
    val f = new FileReader("aws.credentials")
    val properties = new Properties()
    try {
      properties.load(f)
    } finally {
      f.close()
    }
    properties
  }

  /*private def makeJar(state: State) = {
  val toCollect: File = new File("target/scala-2.10/classes")
  val targetPath = toCollect.getPath

  def removePrefix(file: File): (File, String) = (file, file.getPath.substring(targetPath.length, file.getPath.length))

  val collected: Seq[(File, String)] = collectClasses(toCollect).map(removePrefix(_))
  val jar = s"emr-job.jar"

  collected.foreach(file => state.log.log(Level.Info, s"Adding: ${file._2}"))
  val jarFile = new File(jar).getAbsoluteFile

  Package.makeJar(collected, jarFile, new Manifest, state.log)
  state.log.log(Level.Info, s"Generated jar: $jar")
  jarFile
}*/
}
