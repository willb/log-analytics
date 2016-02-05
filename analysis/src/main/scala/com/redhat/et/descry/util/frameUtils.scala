/*
 * frameUtils.scala
 * 
 * author:  William Benton <willb@redhat.com>
 *
 * Copyright (c) 2016 Red Hat, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.redhat.et.descry.util;

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object FrameUtils {
  def destructure(struct: StructType, prefix: Option[String]=None): Array[String] = { 
    struct.fields flatMap {
      case StructField(name, sub@StructType(_), _, _) => destructure(sub, Some(s"${prefix.getOrElse("")}`${name}`."))
      case StructField(name, _, _, _) => Array(s"${prefix.getOrElse("")}`${name}`")
    }
  }

  def flatten(df: DataFrame) = {
    val cols = destructure(df.schema).toList map { name => column(name).as(name.replace(".", "_").replace("`", "")) }
    df.select(cols : _*)
  }

  def mediate(left: DataFrame, right: DataFrame) = {
    val leftCols = destructure(left.schema).toList
    val rightCols = destructure(right.schema)
    val keptSet = Set(leftCols : _*) intersect Set(rightCols : _*)
    val first::rest = leftCols filter { col => keptSet(col) }
    left.select(first, rest : _*) unionAll right.select(first, rest : _*)
  }
}