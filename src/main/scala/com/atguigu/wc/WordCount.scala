package com.atguigu.wc

import org.apache.flink.api.scala._

object WordCount {
  def main(args: Array[String]): Unit = {
    val environment = ExecutionEnvironment.getExecutionEnvironment
    val inputDS: DataSet[String] = environment.readTextFile("input")
    val wordCount: AggregateDataSet[(String, Int)] = inputDS.flatMap(_.split(" "))
      .map((_, 1))
      .groupBy(0)
      .sum(1)
    wordCount.print()

  }

}
