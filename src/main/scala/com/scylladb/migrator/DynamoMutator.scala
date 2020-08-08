package com.scylladb.migrator

import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model._
import zio._
import zio.duration._
import zio.stream._
import zio.test.{Gen, Sized}

import scala.jdk.CollectionConverters._

object DynamoMutator extends App {
  def run(args: List[String]): URIO[ZEnv, ExitCode] =
    Dynamo.client
      .use { client =>
        Dynamo.createTable(client, "mutator_table") *>
          DataGenerator
            .mutationStream("mutator_table")
            .throttleShape(1, 5.second)(_.size)
            .mapM { req =>
              console.putStrLn(s"Going to apply:\n${req.requestItems()}") *>
                Task.fromCompletionStage(client.batchWriteItem(req))
            }
            .runDrain
      }
      .exitCode
      .provideCustomLayer(Sized.live(1000))
}

object Dynamo {
  def client =
    Task(DynamoDbAsyncClient.create()).toManaged(client => UIO(client.close()))

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

}

object DataGenerator {
  val strAttr =
    Gen.alphaNumericString.map(AttributeValue.builder().s(_).build())
  val numAttr =
    Gen.anyFloat.map(l => AttributeValue.builder().n(l.toString).build())
  val boolAttr = Gen.boolean.map(AttributeValue.builder().bool(_).build())
  val binaryAttr = Gen.anyASCIIString.map(str =>
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
          WriteRequest
            .builder()
            .putRequest(_.item((keys ++ attrs).asJava))
            .build()
        }
      else
        keys.map(keys =>
          WriteRequest.builder().deleteRequest(_.key(keys.asJava)).build()
        )
    }

  def mutationStream(tableName: String) =
    ZStream.fromEffect(Ref.make(Set[Map[String, AttributeValue]]())).flatMap {
      existingKeys =>
        writeRequest.sample.forever
          .map(_.value)
          .filterM { req =>
            Option(req.deleteRequest()) match {
              case None =>
                val itemKeys = req
                  .putRequest()
                  .item()
                  .asScala
                  .filterKeys(k => k == "sort" || k == "id")
                  .toMap
                existingKeys.update(_ + itemKeys).as(true)

              case Some(deleteReq) =>
                val itemKeys = deleteReq.key().asScala.toMap
                existingKeys.modify { keys =>
                  if (keys(itemKeys)) (true, keys - itemKeys)
                  else (false, keys)
                }
            }
          }
          .grouped(10)
          .map { reqs =>
            val deduped =
              reqs
                .groupMapReduce(req =>
                  Option(req.deleteRequest()) match {
                    case None =>
                      req
                        .putRequest()
                        .item()
                        .asScala
                        .filterKeys(k => k == "sort" || k == "id")
                        .toMap
                    case Some(req) => req.key().asScala.toMap
                  }
                )(identity)((_, r) => r)
                .values

            BatchWriteItemRequest
              .builder()
              .requestItems(Map(tableName -> reqs.asJava).asJava)
              .build()
          }
    }
}
