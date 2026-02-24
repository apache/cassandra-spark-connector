package com.datastax.spark.connector.writer

import java.util.concurrent.atomic.AtomicReference
import java.util.function.UnaryOperator

import com.datastax.spark.connector.rdd.partitioner.CqlTokenRange
import org.apache.spark.util.AccumulatorV2

class TokenRangeAccumulator(acc: AtomicReference[Set[CqlTokenRange[_, _]]])
  extends AccumulatorV2[Set[CqlTokenRange[_, _]], AtomicReference[Set[CqlTokenRange[_, _]]]] {
  override def isZero: Boolean = value.get().isEmpty

  override def copy(): AccumulatorV2[Set[CqlTokenRange[_, _]], AtomicReference[Set[CqlTokenRange[_, _]]]] =
    new TokenRangeAccumulator(new AtomicReference(acc.get))

  override def reset(): Unit =
    acc.set(Set.empty)

  override def add(v: Set[CqlTokenRange[_, _]]): Unit =
    acc.getAndUpdate(
      new UnaryOperator[Set[CqlTokenRange[_, _]]] {
        override def apply(t: Set[CqlTokenRange[_, _]]): Set[CqlTokenRange[_, _]] = t ++ v
      }
    )

  override def merge(other: AccumulatorV2[Set[CqlTokenRange[_, _]],
    AtomicReference[Set[CqlTokenRange[_, _]]]]): Unit =
    acc.getAndUpdate(
      new UnaryOperator[Set[CqlTokenRange[_, _]]] {
        override def apply(t: Set[CqlTokenRange[_, _]]): Set[CqlTokenRange[_, _]] =
          t ++ other.value.get
      }
    )

  override def value: AtomicReference[Set[CqlTokenRange[_, _]]] = acc
}

object TokenRangeAccumulator {
  def empty: TokenRangeAccumulator = new TokenRangeAccumulator(new AtomicReference(Set.empty))
}
