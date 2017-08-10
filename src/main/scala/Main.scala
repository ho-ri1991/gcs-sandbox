import com.google.cloud.storage._

import scala.concurrent.{Await, ExecutionContext, Future}
import java.io.{FileInputStream, IOException}
import java.nio.charset.StandardCharsets.UTF_8

import com.google.auth.oauth2.{GoogleCredentials}
import com.google.cloud.storage.Blob.BlobSourceOption

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration

object Main {

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

  def createObject(bucketName: String, objectPath: String, content: Array[Byte], isProhibitUpdate: Boolean = false, contentType: String = "text/plain", credentialPath: String = "")(implicit executionContext: ExecutionContext): Future[Blob] = Future {
    val storage = getStorage(Some(credentialPath))
    val blobId = BlobId.of(bucketName, objectPath)
    if (isProhibitUpdate) {
      val blob = storage.get(blobId)
      if (blob != null) throw new StorageException(new IOException("object already exists"))
    }
    val blobInfo = BlobInfo.newBuilder(blobId).setContentType(contentType).build()
    if (blobId != null) {
      val blob = storage.create(blobInfo, content)
      if (blob != null) blob
      else throw new StorageException(new IOException("failed to create new object"))
    } else throw new StorageException(new IOException("failed to create new object"))
  }

  def deleteObject(bucketName: String, objectPath: String, credentialPath: String = "")(implicit executionContext: ExecutionContext): Future[Unit] = Future {
    val storage = getStorage(Some(credentialPath))
    val blobId = BlobId.of(bucketName, objectPath)
    val blob = storage.get(blobId)
    if (blob != null){
      val isDeleted = blob.delete(BlobSourceOption.generationMatch())
      if (isDeleted) ()
      else throw new StorageException(new IOException("failed to delete object"))
    } else throw new StorageException(new IOException("failed to get object"))
  }

  def listAll(bucketName: String, prefix: String, credentialPath: String = "")(implicit executionContext: ExecutionContext): Future[IndexedSeq[Blob]] = Future {
    val storage = getStorage(Some(credentialPath))

    val bucket = storage.get(bucketName)

    val option = Storage.BlobListOption.prefix(prefix)
    val blobs = bucket.list(option)

    val blobItr = blobs.iterateAll().iterator()
    val ans = collection.mutable.ArrayBuffer.empty[Blob]
    while(blobItr.hasNext){
      ans += blobItr.next()
    }
    ans.toIndexedSeq
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
    val createObjectPath = "path/to/object"
    val prefix = "prefix/"
    val credentialPath = "/path/to/credential"

    val done = listAll(bucketName, prefix, credentialPath).map(_.foreach(println))
//    val done = getContent(bucketName, objectPath, credentialPath).map{ content => println(new String(content, UTF_8))}
//    val done = createObject(bucketName, createObjectPath, "Hello, GCS!".getBytes(UTF_8), false, "text/plain", credentialPath)
//    val done = deleteObject(bucketName, createObjectPath, credentialPath)

    done.onFailure{
      case ex: Throwable => println(ex)
    }

    Await.result(done, Duration.Inf)
  }

}
