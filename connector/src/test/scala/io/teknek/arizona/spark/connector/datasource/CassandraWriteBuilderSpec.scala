package io.teknek.arizona.spark.connector.datasource

import org.apache.spark.sql.internal.connector.SupportsStreamingUpdateAsAppend
import org.scalatest.{FlatSpec, Matchers}

class CassandraWriteBuilderSpec extends FlatSpec with Matchers {

  "CassandraWriteBuilder" should "advertise Spark update-mode support" in {
    classOf[SupportsStreamingUpdateAsAppend].isAssignableFrom(classOf[CassandraWriteBuilder]) shouldBe true
  }
}
