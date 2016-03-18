/*
 * som.scala
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

import breeze.linalg._

object Neighborhood {
  private [this] def gaussian(distance: Double, sigma: Double): Double = math.exp(- (distance * distance) / (sigma * sigma))
  
  /** Returns a <tt>dim</tt>-element vector of the values for the one-dimensional Gaussian neighborhood function, centered at <tt>c</tt> */
  def vec(c: Int, dim: Int, sigma: Double): DenseVector[Double] = {
    // weights for each distance
    val weights = (0 to math.max(c, math.abs(dim - c))).map { x => gaussian(x.toDouble, sigma) }
    DenseVector((0 to dim).map { x => weights(math.abs(x - c))}.toArray)
  }
  
  /** Returns a <tt>xdim</tt>*<tt>ydim</tt>-element matrix of the values for the two-dimensional Gaussian neighborhood function, centered at <tt>xc</tt>, <tt>yc</tt> */
  def mat(xc: Int, xdim: Int, xsigma: Double, yc: Int, ydim: Int, ysigma: Double): DenseMatrix[Double] = {
    vec(yc, ydim, ysigma) * vec(xc, xdim, xsigma).t
  }
}

case class SomTrainingState(counts: DenseVector[Double], weights: DenseVector[DenseVector[Double]])

class SOM(val xdim: Int, val ydim: Int, val fdim: Int, entries: DenseVector[DenseVector[Double]]) extends Serializable {
  import breeze.numerics._
  
  val norms = {
    val ea = entries.toArray
    ea zip ea.map(norm(_))
  }
  
  def closest(vec: DenseVector[Double]): Int = {
    val vn = norm(vec)
    norms
      .zipWithIndex
      .map { 
        case ((e: DenseVector[Double], en: Double), i: Int) => (i, math.min(1.0, math.max(-1.0, (e dot vec) / (en * vn))))
      }
      .reduce { (a: Tuple2[Int, Double], b: Tuple2[Int, Double]) => if (a._2 > b._2) a else b }
      ._1 
  }
}

object SOM {
  /** initialize a self-organizing map with random weights */
  def random(xdim: Int, ydim: Int, fdim: Int, seed: Option[Int] = None): SOM = {
    // nb: could/should use breeze PRNGs?
    val rng = seed.map { i => new scala.util.Random(i) }.getOrElse(new scala.util.Random())
    val randomMap = DenseVector.fill[DenseVector[Double]](xdim * ydim)(DenseVector.fill[Double](fdim)(rng.nextDouble()))

    new SOM(xdim, ydim, fdim, randomMap)
  }
}
