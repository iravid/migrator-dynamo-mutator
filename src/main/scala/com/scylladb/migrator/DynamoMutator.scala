package com.scylladb.migrator

import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model._
import zio._
import zio.duration._
import zio.stream._
import zio.test.{Gen, Sized}

import scala.jdk.CollectionConverters._
import software.amazon.awssdk.core.util.PaginatorUtils
import software.amazon.awssdk.core.pagination.sync.PaginatedItemsIterable
import java.net.URI
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.auth.credentials.AwsCredentials
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials

object DynamoMutator extends App {
  def run(args: List[String]): URIO[ZEnv, ExitCode] =
    (for {
      _ <- Dynamo.client.use { remoteClient =>
        Dynamo.createTable(remoteClient, "mutator_table") *>
          console.putStrLn(
            "Starting table mutations; hit any key to stop"
          ) *>
          DataGenerator
            .mutationStream("mutator_table")
            .throttleShape(1, 5.second)(_.size)
            .mapM { req =>
              Task
                .fromCompletionStage(remoteClient.batchWriteItem(req))
                .as(req.requestItems().get("mutator_table").size)
            }
            .interruptWhen(console.getStrLn)
            .run(Sink.foldLeftM(0L) {
              case (acc, el) =>
                val newCount = acc + el
                console
                  .putStrLn(s"Applied ${newCount} mutations so far")
                  .as(newCount)
            })
      }
      _ <- (for {
          remoteResults <- Dynamo.client.use(
            Dynamo.scan(_, "mutator_table").run(Sink.collectAllToSet)
          )
          localResults <- Dynamo.local.use(
            Dynamo.scan(_, "mutator_table").run(Sink.collectAllToSet)
          )
          _ <- console.putStrLn(
            s"remote -- local: ${(remoteResults -- localResults).size}"
          )
          _ <- console.putStrLn(
            s"local -- remote: ${(localResults -- remoteResults).size}"
          )

        } yield ()).repeat(Schedule.fixed(10.seconds) && Schedule.recurs(5))
    } yield ()).exitCode
      .provideCustomLayer(Sized.live(1000))
}

object Dynamo {
  def client =
    Task(DynamoDbAsyncClient.create()).toManaged(client => UIO(client.close()))

  def local =
    Task(
      DynamoDbAsyncClient
        .builder()
        .endpointOverride(URI.create("http://localhost:8000"))
        .credentialsProvider(
          StaticCredentialsProvider.create(
            AwsBasicCredentials.create("empty", "empty")
          )
        )
        .build()
    ).toManaged(c => UIO(c.close()))

  def createTable(client: DynamoDbAsyncClient, tableName: String) =
    Task.fromCompletionStage {
      client.createTable(
        _.attributeDefinitions(
          AttributeDefinition
            .builder()
            .attributeName("id")
            .attributeType(ScalarAttributeType.S)
            .build(),
          AttributeDefinition
            .builder()
            .attributeName("sort")
            .attributeType(ScalarAttributeType.N)
            .build()
        )
          .keySchema(
            KeySchemaElement
              .builder()
              .attributeName("id")
              .keyType(KeyType.HASH)
              .build(),
            KeySchemaElement
              .builder()
              .attributeName("sort")
              .keyType(KeyType.RANGE)
              .build()
          )
          .tableName(tableName)
          .provisionedThroughput(
            ProvisionedThroughput
              .builder()
              .readCapacityUnits(10)
              .writeCapacityUnits(10)
              .build()
          )
      )
    }

  def scan(client: DynamoDbAsyncClient, tableName: String) =
    ZStream.unwrap {
      for {
        queue <- Queue.unbounded[Take[Throwable, Map[String, AttributeValue]]]
        runtime <- ZIO.runtime[Any]
        res <- Task {
          client
            .scanPaginator(_.tableName(tableName).consistentRead(true))
            .subscribe { response =>
              runtime.unsafeRun(
                queue.offer(
                  Take.chunk(
                    Chunk
                      .fromIterable(response.items.asScala.map(_.asScala.toMap))
                  )
                )
              )
            }
        }
        _ <-
          Task
            .fromCompletionStage(res)
            .foldCause(Take.halt, _ => Take.end)
            .tap(queue.offer)
            .forkDaemon
      } yield ZStream.fromQueue(queue).flattenTake
    }

}

object DataGenerator {
  val strAttr =
    Gen.alphaNumericString
      .filter(_.nonEmpty)
      .map(AttributeValue.builder().s(_).build())
  val numAttr =
    Gen.anyFloat.map(l => AttributeValue.builder().n(l.toString).build())
  val boolAttr = Gen.boolean.map(AttributeValue.builder().bool(_).build())
  val binaryAttr = Gen.anyASCIIString
    .filter(_.nonEmpty)
    .map(str =>
      AttributeValue.builder().b(SdkBytes.fromUtf8String(str)).build()
    )

  val keys =
    Gen
      .crossAll(
        Map(
          "id" -> Gen
            .char('a', 'z')
            .map(c => AttributeValue.builder().s(c.toString).build()),
          "sort" -> Gen
            .long(0, 50)
            .map(l => AttributeValue.builder().n(l.toString).build())
        ).map {
          case (name, attr) => attr.map(name -> _)
        }
      )
      .map(_.toMap)

  val attrs =
    Gen
      .crossAll(
        Map(
          "str" -> strAttr,
          "num" -> numAttr,
          "bool" -> boolAttr,
          "bin" -> binaryAttr
        ).map {
          case (name, attr) => attr.map(name -> _)
        }
      )
      .map(_.toMap)

  val writeRequest =
    Gen.boolean.flatMap { isPut =>
      if (isPut)
        keys.crossWith(attrs) { (keys, attrs) =>
          (
            keys,
            true,
            WriteRequest
              .builder()
              .putRequest(_.item((keys ++ attrs).asJava))
              .build()
          )
        }
      else
        keys.map(keys =>
          (
            keys,
            false,
            WriteRequest.builder().deleteRequest(_.key(keys.asJava)).build()
          )
        )
    }

  def mutationStream(tableName: String) =
    ZStream.fromEffect(Ref.make(Set[Map[String, AttributeValue]]())).flatMap {
      existingKeys =>
        writeRequest.sample.forever
          .map(_.value)
          .filterM {
            case (itemKeys, isPut, req) =>
              if (isPut)
                existingKeys.update(_ + itemKeys).as(true)
              else
                existingKeys.modify { keys =>
                  if (keys(itemKeys)) (true, keys - itemKeys)
                  else (false, keys)
                }
          }
          .grouped(10)
          .map { reqs =>
            val deduped =
              reqs
                .groupMapReduce(req => req._1)(identity)((_, r) => r)
                .values
                .map(_._3)
                .asJavaCollection

            BatchWriteItemRequest
              .builder()
              .requestItems(Map(tableName -> deduped).asJava)
              .build()
          }
    }
}
