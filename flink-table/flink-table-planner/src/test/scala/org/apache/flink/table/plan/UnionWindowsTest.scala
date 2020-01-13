/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.plan

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.table.api.Types
import org.apache.flink.table.api.scala._
import org.apache.flink.table.typeutils.TimeIndicatorTypeInfo
import org.apache.flink.table.utils.TableTestBase
import org.apache.flink.types.Row
import org.junit.Test

class UnionWindowsTest extends TableTestBase {

  @Test
  def testUnionDifferentWindows(): Unit = {
    val tablesTypeInfo = Types.ROW(
      Array("timestamp"),
      Array[TypeInformation[_]](TimeIndicatorTypeInfo.ROWTIME_INDICATOR)
    )

    val util = streamTestUtil()
    util.addTable[Row]("table_display", 'timestamp)(tablesTypeInfo)

    val sqlQuery =
      """
        |WITH counts_2h AS (
        |    SELECT
        |        COUNT(0) as c,
        |        HOP_ROWTIME(`timestamp`, INTERVAL '1' HOUR, INTERVAL '2' HOUR) as `timestamp`
        |    FROM table_display
        |    GROUP BY HOP(`timestamp`, INTERVAL '1' HOUR, INTERVAL '2' HOUR)
        |),
        |
        |counts_6h AS (
        |    SELECT
        |        COUNT(0) as c,
        |        HOP_ROWTIME(`timestamp`, INTERVAL '1' HOUR, INTERVAL '6' HOUR) as `timestamp`
        |    FROM table_display
        |    GROUP BY HOP(`timestamp`, INTERVAL '1' HOUR, INTERVAL '6' HOUR)
        |)
        |
        |(SELECT * FROM counts_6h)
        |UNION ALL
        |(SELECT * FROM counts_2h)
        |""".stripMargin

    // We expect both sides of the upper-level 'DataStreamGroupWindowAggregate' to have different sliding windows (6hrs and 2hrs),
    // but the planner gives a plan where both sides have the same sliding window.
    val expected =
      """DataStreamUnion(all=[true], union all=[c, timestamp]): rowcount = 200.0, cumulative cost = {800.0 rows, 802.0 cpu, 0.0 io}, id = 204
        |  DataStreamCalc(select=[c, w$rowtime AS timestamp]): rowcount = 100.0, cumulative cost = {300.0 rows, 301.0 cpu, 0.0 io}, id = 201
        |    DataStreamGroupWindowAggregate(window=[SlidingGroupWindow('w$, 'timestamp, 21600000.millis, 3600000.millis)], select=[COUNT(*) AS c, start('w$) AS w$start, end('w$) AS w$end, rowtime('w$) AS w$rowtime, proctime('w$) AS w$proctime]): rowcount = 100.0, cumulative cost = {200.0 rows, 201.0 cpu, 0.0 io}, id = 200
        |      DataStreamScan(id=[1], fields=[timestamp]): rowcount = 100.0, cumulative cost = {100.0 rows, 101.0 cpu, 0.0 io}, id = 199
        |  DataStreamCalc(select=[c, w$rowtime AS timestamp]): rowcount = 100.0, cumulative cost = {300.0 rows, 301.0 cpu, 0.0 io}, id = 203
        |    DataStreamGroupWindowAggregate(window=[SlidingGroupWindow('w$, 'timestamp, 7200000.millis, 3600000.millis)], select=[COUNT(*) AS c, start('w$) AS w$start, end('w$) AS w$end, rowtime('w$) AS w$rowtime, proctime('w$) AS w$proctime]): rowcount = 100.0, cumulative cost = {200.0 rows, 201.0 cpu, 0.0 io}, id = 202
        |      DataStreamScan(id=[1], fields=[timestamp]): rowcount = 100.0, cumulative cost = {100.0 rows, 101.0 cpu, 0.0 io}, id = 199
        |""".stripMargin

    util.verifySql(sqlQuery, expected)
  }

}
