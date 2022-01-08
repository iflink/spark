/*
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

package org.apache.spark.scheduler

import java.nio.ByteBuffer

import scala.language.existentials

import org.apache.spark._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.shuffle.ShuffleWriter

/**
* A ShuffleMapTask divides the elements of an RDD into multiple buckets (based on a partitioner
* specified in the ShuffleDependency).
*
* See [[org.apache.spark.scheduler.Task]] for more information.
*
 * @param stageId id of the stage this task belongs to
 * @param taskBinary broadcast version of of the RDD and the ShuffleDependency. Once deserialized,
 *                   the type should be (RDD[_], ShuffleDependency[_, _, _]).
 * @param partition partition of the RDD this task is associated with
 * @param locs preferred task execution locations for locality scheduling
 */
/**
 * 一个ShuffleMapTask任务会将一个RDD的元素,切分为多个bucket
 * 基于在ShuffleDependency中指定的partitioner,默认为HashPartitioner
 */
private[spark] class ShuffleMapTask(
    stageId: Int,
    taskBinary: Broadcast[Array[Byte]],
    partition: Partition,
    @transient private var locs: Seq[TaskLocation])
  extends Task[MapStatus](stageId, partition.index) with Logging {

  /** A constructor used only in test suites. This does not require passing in an RDD. */
  def this(partitionId: Int) {
    this(0, null, new Partition { override def index = 0 }, null)
  }

  @transient private val preferredLocs: Seq[TaskLocation] = {
    if (locs == null) Nil else locs.toSet.toSeq
  }

  /**
   * 最重要的一点是：ShuffleMapTask的runTask有MapStatus的返回值
   */
  override def runTask(context: TaskContext): MapStatus = {
    // Deserialize the RDD using the broadcast variable.
    /**
     * 对task需要处理的RDD相关的数据做一些反序列化的操作
     * 
     * 问题：怎么获取RDD的呢？
     * 解答：多个task运行在多个executor中,都是并行运行或并发运行的,可能都不在同一个地方运行。
     *       但是,一个stage的task, 其实要处理的RDD是一样的,所以task怎么提取自己需要处理的数据呢？
     *       答案：通过broadcast variable直接提取
     */
    val ser = SparkEnv.get.closureSerializer.newInstance()
    val (rdd, dep) = ser.deserialize[(RDD[_], ShuffleDependency[_, _, _])](
      ByteBuffer.wrap(taskBinary.value), Thread.currentThread.getContextClassLoader)

    metrics = Some(context.taskMetrics)
    var writer: ShuffleWriter[Any, Any] = null
    try {
      // 获取ShuffleManager,从而获取ShuffleWriter
      val manager = SparkEnv.get.shuffleManager
      writer = manager.getWriter[Any, Any](dep.shuffleHandle, partitionId, context)
      /**
       * 核心实现
       * 1.调用了RDD的iterator方法, 并且传入当前task需要处理的partition。
       * 2.最核心的逻辑就在RDD的iterator方法中, 实现了针对RDD的某个partition执行自定义的算子或者函数。
       * 3.执行完自定义的算子之后,即针对RDD的partition执行了处理, 返回的数据通过HashPartition进行分区,然后应用ShuffleWriter写入对应的bucket分区
       */
      writer.write(rdd.iterator(partition, context).asInstanceOf[Iterator[_ <: Product2[Any, Any]]])
      /**
       * 返回结果为MapStatus
       * MapStatus封装了ShuffleMapTask计算后的数据存储的信息,也就是BlockManager相关的信息。
       * BlockManager：Spark底层的内存、数据、磁盘数据管理组件
       */
      return writer.stop(success = true).get
    } catch {
      case e: Exception =>
        try {
          if (writer != null) {
            writer.stop(success = false)
          }
        } catch {
          case e: Exception =>
            log.debug("Could not stop writer", e)
        }
        throw e
    }
  }

  override def preferredLocations: Seq[TaskLocation] = preferredLocs

  override def toString = "ShuffleMapTask(%d, %d)".format(stageId, partitionId)
}
