/*
 * metaFeatures.scala
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

package com.redhat.et.descry.meta;

import com.redhat.et.descry.util.FrameUtils._

import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer, VectorAssembler}
import org.apache.spark.sql.{DataFrame, Column}
import org.apache.spark.sql.functions._

object MetaFeatures {
  val expectedColumns = Set(
    "@timestamp", "@version", "geoip_location", "hostname", "ipaddr4", "ipaddr6", 
    "level", "message", "pid", "rsyslog_app-name", "rsyslog_facility", "rsyslog_fromhost", 
    "rsyslog_fromhost-ip", "rsyslog_inputname", "rsyslog_msgid", "rsyslog_programname", 
    "rsyslog_protocol-version", "rsyslog_structured-data", "rsyslog_timegenerated"
  )
  
  val factorColumns = Set(
    "hostname", "level", "rsyslog_app-name", "rsyslog_facility", "rsyslog_programname", "rsyslog_inputname"
  )
  
  def oneHotify(df: DataFrame, inputCol: String): DataFrame = {
    val dfNormalized = df.select(df.columns map { 
      case col if col == inputCol => when(isnull(column(col)), "NULL").otherwise(lower(column(col))).as(col) 
      case col => column(col)
    } : _*)
    
    val indexer = new StringIndexer()
      .setInputCol(inputCol)
      .setOutputCol(s"STRINGINDEX_${inputCol}")
      .fit(dfNormalized)
    
    val indexed = indexer.transform(dfNormalized)
    
    val encoder = new OneHotEncoder()
      .setInputCol(s"STRINGINDEX_${inputCol}")
      .setOutputCol(s"ONEHOT_${inputCol}")
    
    val result = encoder.transform(indexed)
    
    result.select(result.columns collect { case col if !col.contains("STRINGINDEX_") => column(col)} : _*)
  }
  
  def oneHotify(df: DataFrame, inputCol: String, inputCols: String*): DataFrame = {
    val availableColumns = Set(df.columns : _*)
    (inputCol :: inputCols.toList).filter { col => availableColumns.contains(col) }.foldLeft(df)((frame: DataFrame, col: String) => oneHotify(frame, col))
  }
  
  def vectorize(df: DataFrame, columns: Set[String] = factorColumns) = {
    val availableColumns = Set(df.columns : _*)
    val oneHots = (availableColumns intersect columns).foldLeft(df)((frame, col) => oneHotify(frame, col))
    val withDerivedFeatures = new VectorAssembler()
      .setInputCols(oneHots.columns collect { case c if c.contains("ONEHOT_") => c })
      .setOutputCol("__GENERATED_FEATURES")
      .transform(oneHots)
    
    withDerivedFeatures.select(withDerivedFeatures.columns collect { case c if !c.contains("ONEHOT") => column(c)} : _*)
  }
}