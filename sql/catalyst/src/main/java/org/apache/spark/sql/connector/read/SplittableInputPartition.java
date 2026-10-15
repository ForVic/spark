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

package org.apache.spark.sql.connector.read;

import org.apache.spark.annotation.Evolving;

/**
 * A mix-in for input partitions that Spark may split into smaller input partitions, for example
 * to read one large input partition with several threads of an executor (see
 * {@code spark.sql.execution.morsel.enabled}).
 * <p>
 * The input partitions that {@link #split(long)} returns must together hold exactly the records of
 * this input partition, each record once, and Spark must be able to read each of them with the
 * {@link PartitionReaderFactory} that reads this input partition, in the same (row-based or
 * columnar) mode. Spark may read them in any order and concurrently, on different threads, and
 * makes no promise about the order of the records it reads. Spark does not split input partitions
 * of scans that report an ordering or a key-grouped partitioning.
 * <p>
 * Input partitions that also implement {@link HasPartitionStatistics} should report their size,
 * which Spark uses to balance the work between threads.
 *
 * @since 4.2.0
 */
@Evolving
public interface SplittableInputPartition extends InputPartition {

  /**
   * Splits this input partition into input partitions of about {@code targetSizeInBytes} bytes
   * each. Returns this input partition alone if it cannot be split.
   */
  InputPartition[] split(long targetSizeInBytes);
}
