/*
 * Copyright (c) 2013-2020 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
 */
package com.snowplowanalytics.snowplow.collectors.scalastream
package sinks

import cats.syntax.either._
import com.aliyun.datahub.client.auth.AliyunAccount
import com.aliyun.datahub.client.common.DatahubConfig
import com.aliyun.datahub.client.http.HttpConfig
import com.aliyun.datahub.client.model.{BlobRecordData, RecordEntry, RecordType}
import com.aliyun.datahub.client.{DatahubClient, DatahubClientBuilder}
import com.snowplowanalytics.snowplow.collectors.scalastream.model._

import scala.collection.JavaConverters._
import scala.util._

/** AliyunDataHubSink companion object with factory method */
object AliyunDataHubSink {
  def createAndInitialize(
    config: AliyunDataHub,
    topicName: String
  ): Either[Throwable, AliyunDataHubSink] =
    for {
      client <- createClient(config)
      _ <- topicExists(client, config.project, topicName).flatMap { b =>
        if (b) b.asRight
        else new IllegalArgumentException(s"Aliyun DataHub topic $topicName doesn't exist").asLeft
      }
    } yield new AliyunDataHubSink(client, config.project, topicName)

  /**
    * Instantiates a DataHub Client on an existing topic with the given configuration options.
    * This can fail if the publisher can't be created.
    * @return a DataHub client or an error
    */
  private def createClient(
    config: AliyunDataHub
  ): Either[Throwable, DatahubClient] =
    Either.catchNonFatal {
      val builder = DatahubClientBuilder
        .newBuilder()
        .setDatahubConfig(
          new DatahubConfig(config.endpoint, new AliyunAccount(config.accessId, config.accessKey), config.enableBinary)
        )
      if (config.httpConfig.isDefined) {
        val hc         = config.httpConfig.get
        val httpConfig = new HttpConfig
        httpConfig
          .setConnTimeout(hc.connTimeout)
          .setMaxRetryCount(hc.maxRetryCount)
          .setConnTimeout(hc.connTimeout)
          .setDebugRequest(hc.debugRequest)
          .setEnableH2C(hc.enableH2C)
          .setEnablePbCrc(hc.enablePbCrc)
        if (hc.compressType.isDefined) {
          hc.compressType.get match {
            case AliyunDataHubHttpCompressTypeLZ4()     => httpConfig.setCompressType(HttpConfig.CompressType.LZ4)
            case AliyunDataHubHttpCompressTypeDeflate() => httpConfig.setCompressType(HttpConfig.CompressType.DEFLATE)
          }
        }
        if (hc.proxyUri.isDefined) {
          httpConfig.setProxyUri(hc.proxyUri.get)
          if (hc.proxyUsername.isDefined) {
            httpConfig.setProxyUsername(hc.proxyUsername.get)
          }
          if (hc.proxyPassword.isDefined) {
            httpConfig.setProxyPassword(hc.proxyPassword.get)
          }
        }
        if (hc.networkInterface.isDefined) {
          httpConfig.setNetworkInterface(hc.networkInterface.get)
        }
      }
      builder.build()
    }

  /** Checks that a DataHub topic exists **/
  private def topicExists(client: DatahubClient, projectId: String, topicName: String): Either[Throwable, Boolean] =
    Either.catchNonFatal({
      val topicResult = client.getTopic(projectId, topicName)
      if (topicResult.getRecordType != RecordType.BLOB)
        throw new IllegalStateException(s"The record type for topic $topicName is not BLOB")
      topicResult.getTopicName == topicName
    })
}

/**
  * Aliyun DataHub Sink for the Scala collector
  */
class AliyunDataHubSink private (client: DatahubClient, project: String, topicName: String) extends Sink {

  // https://help.aliyun.com/document_detail/47441.html
  // maximum size of a DataHub message is 4MB
  override val MaxBytes: Int = 4000000

  /**
    * Store raw events in the DataHub topic
    * @param events The list of events to send
    * @param key The partition key (unused)
    */
  override def storeRawEvents(events: List[Array[Byte]], key: String): List[Array[Byte]] = {
    if (events.nonEmpty) {
      log.debug(s"Writing ${events.size} Thrift records to Aliyun DataHub topic $topicName")
      val records = events.map { event =>
        val entry = new RecordEntry
        entry.addAttribute("key", key)
        entry.setRecordData(new BlobRecordData(event))
        entry
      }
      client.putRecords(project, topicName, records.asJava)
    } else log.debug(s"Skip empty records")
    Nil
  }
}
