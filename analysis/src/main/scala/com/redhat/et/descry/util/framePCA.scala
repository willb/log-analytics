/*
 * framePCA.scala
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

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

import org.apache.spark.ml.param._
import org.apache.spark.ml.Transformer

import org.apache.spark.mllib.linalg.{Vector => VEC, Vectors, VectorUDT}
import org.apache.spark.mllib.feature.PCA

import org.apache.spark.sql.types._
import org.apache.spark.sql.Row

trait PCAParams extends Params {
  val inputCol = new Param[String](this, "inputCol", "input column")
  val outputCol = new Param[String](this, "outputCol", "output column")
  val pcs = new IntParam(this, "pcs", "number of principal components")

  def pvals(pm: ParamMap) = (
    pm.getOrElse(inputCol, "features"),
    pm.getOrElse(outputCol, "principalComponents"),
    pm.getOrElse(pcs, 2)
  )
}

class PCATransformer(override val uid: String, private val extra: ParamMap = ParamMap())
    extends Transformer with PCAParams {
  val VT = new VectorUDT()
  
  def copy(_extra: ParamMap): PCATransformer = {
    new PCATransformer(uid, extra ++ _extra)
  }
  
  def transformSchema(schema: StructType) = {
    val outc = extractParamMap(extra).getOrElse(outputCol, "featurePCs")
    StructType(schema.fields ++ Seq(StructField(outc, VT, true)))
  }

  def transform(df: Dataset[_]) = {
    val (inCol, outCol, pcs) = pvals(extractParamMap(extra))
    val featureRDD = df.select(inCol).rdd.map { case Row(v: VEC) => v }
    featureRDD.cache
    
    val model = df.sqlContext.sparkContext.broadcast(new PCA(pcs).fit(featureRDD))
    
    featureRDD.unpersist()

    val transform = udf({ a: VEC => model.value.transform(a)}, VT)
    
    df.withColumn(outCol, transform(df(inCol)))
  }
}
