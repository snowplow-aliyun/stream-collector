/*
 * Copyright (c) 2013-2020 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0, and
 * you may not use this file except in compliance with the Apache License
 * Version 2.0.  You may obtain a copy of the Apache License Version 2.0 at
 * http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the Apache License Version 2.0 is distributed on an "AS
 * IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the Apache License Version 2.0 for the specific language
 * governing permissions and limitations there under.
 */
package com.snowplowanalytics.snowplow.collectors.scalastream

import cats.syntax.either._

import com.snowplowanalytics.snowplow.collectors.scalastream.model._
import com.snowplowanalytics.snowplow.collectors.scalastream.sinks.AliyunDataHubSink

object AliyunDataHubCollector extends Collector {

  def main(args: Array[String]): Unit = {
    val (collectorConf, akkaConf) = parseConfig(args)

    val sinks: Either[Throwable, CollectorSinks] = for {
      config <- collectorConf.streams.sink match {
        case config: AliyunDataHub => config.asRight
        case _                     => new IllegalArgumentException("Configured sink is not DataHub").asLeft
      }
      goodStream = collectorConf.streams.good
      badStream  = collectorConf.streams.bad
      good <- AliyunDataHubSink.createAndInitialize(config, goodStream)
      bad  <- AliyunDataHubSink.createAndInitialize(config, badStream)
    } yield CollectorSinks(good, bad)

    sinks match {
      case Right(s) => run(collectorConf, akkaConf, s)
      case Left(e)  => throw e
    }
  }
}
