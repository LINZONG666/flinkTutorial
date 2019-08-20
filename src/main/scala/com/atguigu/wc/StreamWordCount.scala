package com.atguigu.wc

import org.apache.flink.streaming.api.scala._

object StreamWordCount {
  def main(args: Array[String]): Unit = {
    val environment = StreamExecutionEnvironment.getExecutionEnvironment

    val testDS: DataStream[String] = environment.socketTextStream("hadoop102",7777)
    val streamWordCount: DataStream[(String, Int)] = testDS.flatMap(_.split("\\s"))
      .map((_, 1))
      .keyBy(0)
      .sum(1)
    streamWordCount.print().setParallelism(1)
    environment.execute("wc")

  }

}
