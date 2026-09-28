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

import org.apache.spark.SparkFunSuite

class TaskCpuReservationsSuite extends SparkFunSuite {
  test("ordinary residency excludes live tasks and is restored on completion or executor loss") {
    val reservations = new TaskCpuReservations
    reservations.start(1, "a", BigDecimal("0.75"), canRelease = false, isPipelined = false)
    reservations.start(2, "a", BigDecimal(1), canRelease = false, isPipelined = true)
    reservations.start(3, "b", BigDecimal(2), canRelease = false, isPipelined = false)
    assert(reservations.ordinaryCpus("a") == BigDecimal("0.75"))
    assert(reservations.ordinaryCpus("b") == 2)
    reservations.finish(1)
    assert(reservations.ordinaryCpus("a") == 0)
    reservations.removeExecutor("b")
    assert(reservations.ordinaryCpus("b") == 0)
    assert(reservations.finish(2) == 1)
  }

  test("release, resume and completion charge CPU once, retaining ordinary task reservations") {
    val reservations = new TaskCpuReservations
    reservations.start(1, "a", BigDecimal(2), canRelease = true, isPipelined = true)
    reservations.start(2, "a", BigDecimal(1), canRelease = false, isPipelined = false)
    assert(reservations.release(2, "a", 1) == 0)
    assert(reservations.release(1, "b", 1) == 0)
    assert(reservations.release(1, "a", 2) == 0)
    assert(reservations.release(1, "a", 1) == 2)
    assert(reservations.release(1, "a", 1) == 0)
    reservations.ready(1, "a", 2)
    assert(reservations.resume("a", 2).isEmpty)
    reservations.ready(1, "a", 1)
    assert(reservations.resume("a", 1).isEmpty)
    assert(reservations.resume("b", 2).isEmpty)
    assert(reservations.resume("a", 2) == Seq((1L, 1L, BigDecimal(2))))
    assert(reservations.resume("a", 2).isEmpty)
    assert(reservations.release(1, "a", 1) == 0)
    assert(reservations.finish(1) == 2)
    assert(reservations.finish(1) == 0)
    assert(reservations.finish(2) == 1)
  }

  test("cancellation and executor loss remove released and pending resumptions") {
    val reservations = new TaskCpuReservations
    reservations.start(1, "a", BigDecimal(1), canRelease = true, isPipelined = true)
    reservations.start(2, "b", BigDecimal(1), canRelease = true, isPipelined = true)
    assert(reservations.release(1, "a", 1) == 1)
    reservations.ready(1, "a", 1)
    assert(reservations.finish(1) == 0)
    reservations.ready(1, "a", 1)
    assert(reservations.resume("a", 1).isEmpty)
    assert(reservations.release(2, "b", 1) == 1)
    reservations.ready(2, "b", 1)
    reservations.removeExecutor("b")
    assert(reservations.resume("b", 1).isEmpty)
    assert(reservations.finish(2) == 0)
  }

  test("resumptions follow readiness order so older tasks cannot monopolize CPU grants") {
    val reservations = new TaskCpuReservations
    (1L to 2L).foreach { id =>
      reservations.start(id, "a", BigDecimal(1), canRelease = true, isPipelined = true)
      reservations.release(id, "a", 1)
    }
    reservations.ready(2, "a", 1)
    reservations.ready(1, "a", 1)
    assert(reservations.resume("a", 1).map(_._1) == Seq(2L))
    reservations.release(2, "a", 2)
    reservations.ready(2, "a", 2)
    assert(reservations.resume("a", 1).map(_._1) == Seq(1L))
  }

  test("fractional CPU resumptions never exceed the executor's remaining capacity") {
    val reservations = new TaskCpuReservations
    (1L to 3L).foreach { id =>
      reservations.start(id, "a", BigDecimal("0.75"), canRelease = true, isPipelined = true)
      assert(reservations.release(id, "a", 1) == BigDecimal("0.75"))
      reservations.ready(id, "a", 1)
    }
    val granted = reservations.resume("a", BigDecimal(2))
    assert(granted.map(_._1) == Seq(1L, 2L))
    assert(granted.map(_._3).sum == BigDecimal("1.5"))
    assert(reservations.resume("a", BigDecimal("0.5")).isEmpty)
    assert(reservations.resume("a", BigDecimal("0.75")).map(_._1) == Seq(3L))
  }
}
