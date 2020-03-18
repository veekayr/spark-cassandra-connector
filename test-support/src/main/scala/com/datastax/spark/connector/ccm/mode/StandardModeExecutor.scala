package com.datastax.spark.connector.ccm.mode

import java.io.File
import java.nio.file.{Files, Path, Paths}
import java.util.concurrent.atomic.AtomicBoolean
import java.util.logging.Logger

import com.datastax.oss.driver.api.core.Version
import com.datastax.spark.connector.ccm.CcmConfig
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

private[mode] trait DefaultExecutor extends ClusterModeExecutor {
  private val logger: Logger = LoggerFactory.getLogger(classOf[StandardModeExecutor])

  private val created = new AtomicBoolean()

  override def start(nodeNo: Int): Unit = {
    val formattedJvmArgs = config.jvmArgs.map(arg => s" --jvm_arg=$arg").mkString(" ")
    execute(s"node$nodeNo", "start", formattedJvmArgs + "--wait-for-binary-proto")
  }

  private def eventually[T](timeoutInSeconds: Int = 20, intervalInMs: Int = 500)(f: => T)(hint: String = ""): T = {
    val start = System.currentTimeMillis()
    val end = start + timeoutInSeconds * 1000

    while ( System.currentTimeMillis() < end) {
      try {
        return f
      } catch { case e: Throwable =>
        logger.warning(s"Tried to execute code, will retry : ${e.getMessage}")
        Thread.sleep(intervalInMs)
      }
    }
    throw new IllegalStateException(s"Unable to complete function in $timeoutInSeconds: $hint")
  }

  override def create(clusterName: String): Unit = {
    if (created.compareAndSet(false, true)) {
      val options = config.installDirectory
        .map(dir => config.createOptions :+ s"--install-dir=${new File(dir).getAbsolutePath}")
        .orElse(config.installBranch.map(branch => config.createOptions :+ s"-v git:${branch.trim().replaceAll("\"", "")}"))
        .getOrElse(config.createOptions :+ s"-v ${config.version}")

      val dseFlag = if (config.dseEnabled) Some("--dse") else None

      val createArgs = Seq("create", clusterName, "-i", config.ipPrefix, (options ++ dseFlag).mkString(" "))

      // Check installed Directory
      val repositoryDir = Paths.get(
        sys.props.get("user.home").get,
        ".ccm",
        "repository",
        config.getDseVersion.getOrElse(config.getCassandraVersion).toString)

      if (Files.exists(repositoryDir)) {
        logger.info(s"Found cached repository dir: $repositoryDir")
        logger.info("Checking for appropriate bin dir")
        eventually()(Files.exists(repositoryDir.resolve("bin")))
        Files.walk(repositoryDir.resolve("bin")).iterator().asScala.foreach(println)
      }

      execute( createArgs: _*)

      eventually(){
        Files.walk(repositoryDir, 1).iterator().asScala.foreach(println)
        Files.exists(repositoryDir.resolve("bin"))
      }("Checking to make sure repository was correctly expanded")

      config.nodes.foreach { i =>
        val addArgs = Seq ("add",
          "-s",
          "-j", config.jmxPort(i).toString,
          "-i", config.ipOfNode(i),
          "--remote-debug-port=0") ++
          dseFlag :+
          s"node$i"

        execute(addArgs: _*)
      }

      config.cassandraConfiguration.foreach { case (key, value) =>
        execute("updateconf", s"$key:$value")
      }
      if (config.getCassandraVersion.compareTo(Version.V2_2_0) >= 0) {
        execute("updateconf", "enable_user_defined_functions:true")
      }
      if (config.dseEnabled) {
        config.dseConfiguration.foreach { case (key, value) =>
          execute("updatedseconf", s"$key:$value")
        }
        config.dseRawYaml.foreach { yaml =>
          executeUnsanitized("updatedseconf", "-y", yaml)
        }
        if (config.dseWorkloads.nonEmpty) {
          execute("setworkload", config.dseWorkloads.mkString(","))
        }
      }
    }
  }
}

private[ccm] class StandardModeExecutor(val config: CcmConfig) extends DefaultExecutor {
  override val dir: Path = Files.createTempDirectory("ccm")
  // remove config directory on shutdown
  dir.toFile.deleteOnExit()
  // remove db artifacts
  override def remove(): Unit = {
    execute("remove")
  }
}
