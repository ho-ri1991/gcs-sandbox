import java.io.{FileInputStream, IOException}
import java.nio.charset.StandardCharsets.UTF_8

import Main.getStorage
import akka.stream.{ActorMaterializer, ActorMaterializerSettings, KillSwitches, Supervision}

import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutor, Future}
import akka.actor.ActorSystem
import akka.stream.scaladsl._
import com.google.auth.oauth2.GoogleCredentials
import com.google.cloud.storage.{BlobId, Storage, StorageException, StorageOptions}

import scala.concurrent.duration._

object AkkaStreamGCSConnector {

  def getStorage(credentialPath: scala.Option[String] = None): Storage = credentialPath match {
    case Some(path) if !path.isEmpty =>
      var credentialStream: FileInputStream = null
      try {
        credentialStream = new FileInputStream(path)
        StorageOptions
          .newBuilder()
          .setCredentials(GoogleCredentials.fromStream(credentialStream))
          .build()
          .getService()
      } catch {
        case ex: Throwable => throw ex
      } finally {
        if (credentialStream != null) credentialStream.close()
      }
    case _ => StorageOptions.getDefaultInstance.getService
  }

  def getContent(bucketName: String, objectPath: String, credentialPath: String = "")(implicit executorContext: ExecutionContext): Future[Array[Byte]] = {
    Future {
      val storage = getStorage(Some(credentialPath))
      val blodId = BlobId.of(bucketName, objectPath)
      val blob = storage.get(blodId)
      if (blob != null) blob.getContent()
      else throw new StorageException(new IOException("failed to get object"))
    }
  }

  def main(args: Array[String]): Unit = {
    val bucketName = "test-bucket"
    val objectPath = "path/to/object"
    val credentialPath = "/path/to/credential"

    implicit val system = ActorSystem("test")
    implicit val mat = ActorMaterializer(
      ActorMaterializerSettings(system).withSupervisionStrategy( ex => ex match {
        case t: Throwable =>
          system.log.error(t, "unexpected error occurred - shutdown application")
          system.log.error(t.getStackTrace.mkString("", "\n", "\n"))
          Supervision.Stop
      })
    )
    implicit val executorContext: ExecutionContextExecutor = system.dispatcher

    val source = Source.fromFuture(getContent(bucketName, objectPath, credentialPath))
    val sink = Sink.ignore

    val bluePrint = source
                    .viaMat(KillSwitches.single)(Keep.right)
                    .map{content => println(new String(content, UTF_8))}
                    .toMat(sink)(Keep.both)

    val (controller, done) = bluePrint.run()
    done.onFailure{
      case ex: Throwable =>
        system.log.error(ex, "unexpected error occurred - shutdown application")
        system.log.info("shutdown actor system on failure")
        Await.ready(system.terminate(), 30 seconds)
    }

    done.flatMap{t =>
      system.log.info("shutdown actor system")
      system.terminate().map(_ => t)
    }

    sys.addShutdownHook{
      println("+++++++")
      controller.shutdown()
    }
  }
}
