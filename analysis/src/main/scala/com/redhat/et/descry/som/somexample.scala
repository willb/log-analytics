/*
 * somexample.scala
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

package com.redhat.et.descry.som

object Example {
  import org.apache.spark.mllib.linalg.{DenseVector => DV}
  import com.redhat.et.descry.som.SOM
  import com.redhat.et.descry.util.ImageWriter
  import org.apache.spark.SparkContext
  
  def apply(xdim: Int, ydim: Int, iterations: Int, sc: SparkContext, exampleCount: Int): SOM = {
    val rnd = new scala.util.Random()
    val colors = Array.fill(exampleCount)(new DV(Array.fill(3)(rnd.nextDouble)).compressed)
    val examples = sc.parallelize(colors).repartition(sc.defaultParallelism * 8)
    
    def writeStep(step: Int, som: SOM) {
      ImageWriter.write(xdim, ydim, som.entries, "som-step-%04d.png".format(step))
    }
    
    com.redhat.et.descry.som.SOM.train(xdim, ydim, 3, iterations, examples, sigmaScale=0.7, hook=writeStep _)
  }
  
  def profile(sc: SparkContext, xdim: Int = 256, ydim: Int = 144, fdim: Int = 8, partitions: Int = 128, iterations: Int = 40, exampleCount: Int = 200000): SOM = {
    val rnd = new scala.util.Random()
    val features = math.max(fdim, 3)
    val colors = Array.fill(exampleCount)(new DV(Array.fill(features)(rnd.nextDouble)).compressed)
    val examples = sc.parallelize(colors).repartition(partitions)
  
    def writeStep(step: Int, som: SOM) { }

    com.redhat.et.descry.som.SOM.train(xdim, ydim, features, iterations, examples, sigmaScale=0.7, hook=writeStep _)
  }
}