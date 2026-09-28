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

import scala.collection.mutable

/** CPU accounting for resident pipelined tasks. Access only on the backend endpoint thread. */
private[spark] final class TaskCpuReservations {
  private case class Reservation(
      executorId: String,
      cpus: BigDecimal,
      canRelease: Boolean,
      isPipelined: Boolean,
      var generation: Long = 0L,
      var held: Boolean = true)

  private val tasks = mutable.HashMap.empty[Long, Reservation]
  private val readyTasks = mutable.LinkedHashSet.empty[Long]
  private val ordinaryCpusByExecutor = mutable.HashMap.empty[String, BigDecimal]

  def start(
      taskId: Long,
      executorId: String,
      cpus: BigDecimal,
      canRelease: Boolean,
      isPipelined: Boolean): Unit = {
    require(!tasks.contains(taskId))
    tasks(taskId) = Reservation(executorId, cpus, canRelease, isPipelined)
    if (!isPipelined) {
      ordinaryCpusByExecutor(executorId) = ordinaryCpus(executorId) + cpus
    }
  }

  def ordinaryCpus(executorId: String): BigDecimal = {
    ordinaryCpusByExecutor.getOrElse(executorId, BigDecimal(0))
  }

  def release(taskId: Long, executorId: String, sequence: Long): BigDecimal = {
    tasks.get(taskId) match {
      case Some(r) if r.canRelease && r.executorId == executorId && r.held &&
          sequence == r.generation + 1 =>
        r.generation = sequence
        r.held = false
        r.cpus
      case _ => BigDecimal(0)
    }
  }

  def ready(taskId: Long, executorId: String, sequence: Long): Unit = {
    tasks.get(taskId).foreach { r =>
      if (r.executorId == executorId && !r.held && r.generation == sequence) {
        readyTasks += taskId
      }
    }
  }

  /** Charge resumptions before offering the remaining capacity to new tasks. */
  def resume(executorId: String, available: BigDecimal): Seq[(Long, Long, BigDecimal)] = {
    var free = available
    val granted = mutable.ArrayBuffer.empty[(Long, Long, BigDecimal)]
    readyTasks.toList.foreach { id =>
      val r = tasks(id)
      if (r.executorId == executorId && r.cpus <= free) {
        free -= r.cpus
        readyTasks -= id
        r.held = true
        granted += ((id, r.generation, r.cpus))
      }
    }
    granted.toSeq
  }

  def finish(taskId: Long): BigDecimal = {
    readyTasks -= taskId
    val reservation = tasks.remove(taskId)
    reservation.filterNot(_.isPipelined).foreach { r =>
      val remaining = ordinaryCpus(r.executorId) - r.cpus
      if (remaining == 0) ordinaryCpusByExecutor.remove(r.executorId)
      else ordinaryCpusByExecutor(r.executorId) = remaining
    }
    reservation.filter(_.held).map(_.cpus).getOrElse(BigDecimal(0))
  }

  def removeExecutor(executorId: String): Unit = {
    tasks.filterInPlace { case (_, r) => r.executorId != executorId }
    readyTasks.filterInPlace(tasks.contains)
    ordinaryCpusByExecutor.remove(executorId)
  }
}
