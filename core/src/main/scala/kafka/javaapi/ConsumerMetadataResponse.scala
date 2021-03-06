/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.javaapi

import java.nio.ByteBuffer

import kafka.cluster.Broker

class ConsumerMetadataResponse(private val underlying: kafka.api.ConsumerMetadataResponse) {

  def errorCode = underlying.errorCode

  def coordinator: Broker = {
    import kafka.javaapi.Implicits._
    underlying.coordinatorOpt
  }

  override def equals(other: Any) = canEqual(other) && {
    val otherConsumerMetadataResponse = other.asInstanceOf[kafka.javaapi.ConsumerMetadataResponse]
    this.underlying.equals(otherConsumerMetadataResponse.underlying)
  }

  def canEqual(other: Any) = other.isInstanceOf[kafka.javaapi.ConsumerMetadataResponse]

  override def hashCode = underlying.hashCode

  override def toString = underlying.toString

}

object ConsumerMetadataResponse {
  def readFrom(buffer: ByteBuffer) = new ConsumerMetadataResponse(kafka.api.ConsumerMetadataResponse.readFrom(buffer))
}
