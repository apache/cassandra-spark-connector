package com.datastax.spark.connector.rdd.reader

import com.datastax.oss.driver.api.core.cql.{AsyncResultSet, Row}
import org.mockito.Mockito._
import org.scalatest.{FlatSpec, Matchers}
import org.scalatestplus.mockito.MockitoSugar

import java.util.concurrent.CompletableFuture
import scala.jdk.CollectionConverters._

class PrefetchingResultSetIteratorSpec extends FlatSpec with Matchers with MockitoSugar {

  "PrefetchingResultSetIterator" should "handle empty pages that are not the last" in {
    val row1 = mock[Row]
    val row2 = mock[Row]
    val row3 = mock[Row]

    val asyncResultSet1 = mock[AsyncResultSet]
    val asyncResultSet2 = mock[AsyncResultSet]
    val asyncResultSet3 = mock[AsyncResultSet]
    val asyncResultSet4 = mock[AsyncResultSet]
    val asyncResultSet5 = mock[AsyncResultSet]

    // First page is empty
    when(asyncResultSet1.currentPage()).thenReturn(Seq.empty[Row].asJava)
    when(asyncResultSet1.hasMorePages()).thenReturn(true)
    when(asyncResultSet1.fetchNextPage()).thenReturn(CompletableFuture.completedFuture(asyncResultSet2))

    // Second page has data
    when(asyncResultSet2.currentPage()).thenReturn(Seq(row1).asJava)
    when(asyncResultSet2.hasMorePages()).thenReturn(true)
    when(asyncResultSet2.fetchNextPage()).thenReturn(CompletableFuture.completedFuture(asyncResultSet3))

    // Third page is empty
    when(asyncResultSet3.currentPage()).thenReturn(Seq.empty[Row].asJava)
    when(asyncResultSet3.hasMorePages()).thenReturn(true)
    when(asyncResultSet3.fetchNextPage()).thenReturn(CompletableFuture.completedFuture(asyncResultSet4))

    // Fourth page has data
    when(asyncResultSet4.currentPage()).thenReturn(Seq(row2, row3).asJava)
    when(asyncResultSet4.hasMorePages()).thenReturn(true)
    when(asyncResultSet4.fetchNextPage()).thenReturn(CompletableFuture.completedFuture(asyncResultSet5))

    // Last page is empty
    when(asyncResultSet5.currentPage()).thenReturn(Seq.empty[Row].asJava)
    when(asyncResultSet5.hasMorePages()).thenReturn(false)

    val iterator = new PrefetchingResultSetIterator(asyncResultSet1)

    val rows = iterator.toList

    rows should contain theSameElementsInOrderAs Seq(row1, row2, row3)

    verify(asyncResultSet1).fetchNextPage()
    verify(asyncResultSet2).fetchNextPage()
    verify(asyncResultSet3).fetchNextPage()
    verify(asyncResultSet4).fetchNextPage()
    verify(asyncResultSet5, never()).fetchNextPage()
  }

  it should "handle a result made of empty pages only" in {
    val asyncResultSet1 = mock[AsyncResultSet]
    val asyncResultSet2 = mock[AsyncResultSet]
    val asyncResultSet3 = mock[AsyncResultSet]

    // First page is empty
    when(asyncResultSet1.currentPage()).thenReturn(Seq.empty[Row].asJava)
    when(asyncResultSet1.hasMorePages()).thenReturn(true)
    when(asyncResultSet1.fetchNextPage()).thenReturn(CompletableFuture.completedFuture(asyncResultSet2))

    // Second page is empty
    when(asyncResultSet2.currentPage()).thenReturn(Seq.empty[Row].asJava)
    when(asyncResultSet2.hasMorePages()).thenReturn(true)
    when(asyncResultSet2.fetchNextPage()).thenReturn(CompletableFuture.completedFuture(asyncResultSet3))

    // Last page is empty
    when(asyncResultSet3.currentPage()).thenReturn(Seq.empty[Row].asJava)
    when(asyncResultSet3.hasMorePages()).thenReturn(false)

    val iterator = new PrefetchingResultSetIterator(asyncResultSet1)

    val rows = iterator.toList

    rows should contain theSameElementsInOrderAs Seq.empty[Row]

    verify(asyncResultSet1).fetchNextPage()
    verify(asyncResultSet2).fetchNextPage()
    verify(asyncResultSet3, never()).fetchNextPage()
  }
}