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
import breeze.numerics._

object Neighborhood {
  private [this] def gaussian(distance: Double, sigma: Double): Double = math.exp(- (distance * distance) / (sigma * sigma))
  
  /** Returns a <tt>dim</tt>-element vector of the values for the one-dimensional Gaussian neighborhood function, centered at <tt>c</tt> */
  def vec(c: Int, dim: Int, sigma: Double): DenseVector[Double] = {
    // weights for each distance
    val weights = (0 to math.max(c, math.abs(dim - c))).map { x => gaussian(x.toDouble, sigma) }
    DenseVector((0 until dim).map { x => weights(math.abs(x - c))}.toArray)
  }
  
  /** Returns a <tt>xdim</tt>*<tt>ydim</tt>-element matrix of the values for the two-dimensional Gaussian neighborhood function, centered at <tt>xc</tt>, <tt>yc</tt> */
  def mat(xc: Int, xdim: Int, xsigma: Double, yc: Int, ydim: Int, ysigma: Double): DenseMatrix[Double] = {
    vec(yc, ydim, ysigma) * vec(xc, xdim, xsigma).t
  }
}

class SOM(val xdim: Int, val ydim: Int, val fdim: Int, entries: DenseVector[DenseVector[Double]]) extends Serializable {
  import breeze.numerics._
  
  val norms = {
    val ea = entries.toArray
    ea.zip(ea.map(norm(_))).zipWithIndex
  }
  
  /** Return the index of the closest vector in the map to the supplied example */
  def closest(example: DenseVector[Double], exampleNorm: Option[Double] = None): Int = {
    val vn = exampleNorm.getOrElse(norm(example))
    norms
      .map { 
        case ((e: DenseVector[Double], en: Double), i: Int) => (i, math.min(1.0, math.max(-1.0, (e dot example) / (en * vn))))
      }
      .reduce { (a: Tuple2[Int, Double], b: Tuple2[Int, Double]) => if (a._2 > b._2) a else b }
      ._1
  }
}

object SOM {
  import breeze.numerics._
  import org.apache.spark.rdd.RDD
  import org.apache.spark.mllib.linalg.{Vector=>SV, DenseVector=>SDV, SparseVector=>SSV}

  private [som] case class SomTrainingState(counts: Array[Int], weights: Array[DenseVector[Double]]) {
    /* destructively updates this state with a new example */
    def update(index: Int, example: DenseVector[Double]) {
      counts(index) = counts(index) + 1
      weights(index) = weights(index) + example
    }
    
    /* destructively merges other into this */
    def combine(other: SomTrainingState) = { 
      (0 until counts.length) foreach { index =>
        this.counts(index) = this.counts(index) + other.counts(index)
        this.weights(index) = this.weights(index) + other.weights(index)
      }
    } 
  }

  /** initialize a self-organizing map with random weights */
  def random(xdim: Int, ydim: Int, fdim: Int, seed: Option[Int] = None): SOM = {
    // nb: could/should use breeze PRNGs?
    val rng = seed.map { s => new scala.util.Random(s) }.getOrElse(new scala.util.Random())
    val randomMap = DenseVector.fill[DenseVector[Double]](xdim * ydim)(DenseVector.fill[Double](fdim)(rng.nextDouble()))

    new SOM(xdim, ydim, fdim, randomMap)
  }
  
  /** Create a new SOM instance with the results of the training state */
  private [som] def step(xdim: Int, ydim: Int, fdim: Int, state: SomTrainingState, xsigma: Double, ysigma: Double) = {
    var weights = DenseVector.fill[DenseVector[Double]](xdim * ydim)(DenseVector.zeros[Double](fdim))
    val seenWeights = DenseVector[DenseVector[Double]](state.weights)
    var neighborhoods = DenseVector.zeros[Double](xdim * ydim)
    
    (0 until xdim * ydim).foreach { idx =>
      val (xc, yc) = (idx % xdim, idx / ydim)
      val counts = state.counts(idx).toDouble
      val hood = Neighborhood.mat(xc, xdim, xsigma, yc, ydim, ysigma).reshape(xdim * ydim, 1).toDenseVector * counts
      neighborhoods :+ hood
      // XXX: the rhs of this doesn't compile but I'm not sure why
      // weights :+ (seenWeights * (hood * state.counts(idx).toDouble))
      // XXX: so we're doing it the hard way
      val update = DenseVector(seenWeights.values.iterator.zip((hood * state.counts(idx).toDouble).values.iterator).map { case (vd, d) => d * vd }.toArray)
      weights :+ update
    }
    
    val newWeights = DenseVector(weights.values.iterator.zip(neighborhoods.values.iterator).map { case (vd, d) => vd / d }.toArray)
    new SOM(xdim, ydim, fdim, newWeights)
  }
  
  def train(xdim: Int, ydim: Int, iterations: Int, examples: RDD[SV]) = ???
}
