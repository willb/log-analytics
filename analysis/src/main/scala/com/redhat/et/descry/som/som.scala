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

object Log {
  import java.util.logging.{Logger, Level}
  val logger = Logger.getLogger("com.redhat.et.descry.som")
  def log(level: Level, msg: => String) {
//    if (logger.getLevel.intValue < level.intValue) {
      logger.log(level, msg)
//    }
  }
  def debug(msg: => String) {
    logger.log(Level.FINE, msg)
  }
}

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

class SOM(val xdim: Int, val ydim: Int, val fdim: Int, _entries: DenseVector[DenseVector[Double]]) extends Serializable {
  import breeze.numerics._
  
  val entries = _entries.toArray
  
  val norms = entries.zip(entries.map(norm(_))).zipWithIndex
  
  /** Return the index of the closest vector in the map to the supplied example */
  def closest(example: Vector[Double], exampleNorm: Option[Double] = None): Int = {
    val vn = exampleNorm.getOrElse(norm(example))
    val result = norms
      .map { 
        case ((e: Vector[Double], en: Double), i: Int) => (i, math.min(1.0, math.max(-1.0, (e dot example) / (en * vn))))
      }
      .reduce { (a: Tuple2[Int, Double], b: Tuple2[Int, Double]) => if (a._2 > b._2) a else b }
      ._1
    Log.debug(s"SOM.closest returned ${result}")
    result
  }
}

object SOM {
  import breeze.numerics._
  import org.apache.spark.rdd.RDD
  import org.apache.spark.mllib.linalg.{Vector=>SV, DenseVector=>SDV, SparseVector=>SSV}
  
  private [som] case class SomTrainingState(counts: Array[Int], weights: Array[DenseVector[Double]]) {
    /* destructively updates this state with a new example */
    def update(index: Int, example: Vector[Double]) = {
      counts(index) = counts(index) + 1
      weights(index) = weights(index) + example
      this
    }
    
    /* destructively merges other into this */
    def combine(other: SomTrainingState) = { 
      (0 until counts.length) foreach { index =>
        this.counts(index) = this.counts(index) + other.counts(index)
        this.weights(index) = this.weights(index) + other.weights(index)
      }
      this
    } 
  }
  
  private [som] object SomTrainingState {
    def empty(dim: Int, fdim: Int): SomTrainingState = SomTrainingState(Array.fill(dim)(0), Array.fill(dim)(DenseVector.zeros[Double](fdim)))
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
      val hood = Neighborhood.mat(xc, xdim, xsigma, yc, ydim, ysigma).reshape(xdim * ydim, 1).toDenseVector
      neighborhoods = neighborhoods :+ (hood * counts)
      val update = DenseVector(hood.toArray.map { hoodDistance => state.weights(idx) * hoodDistance})
      weights = weights :+ update
    }
    
    val newWeights = DenseVector(weights.values.iterator.zip(neighborhoods.values.iterator).map { 
      case (vd, d) if d == 0.0 => DenseVector.zeros[Double](vd.size)
      case (vd, d) => vd / d 
    }.toArray)
    new SOM(xdim, ydim, fdim, newWeights)
  }
  
  @inline private [som] def spark2breeze(vec: SV): Vector[Double] = {
    vec match {
      case sv: SSV => {
        val vb = new VectorBuilder[Double]()
        (sv.indices zip sv.values).foreach { case (idx: Int, v: Double) =>
          vb.add(idx, v)
        }
        vb.toSparseVector()
      }
      
      case dv: SDV => {
        DenseVector(dv.toArray)
      }
    }
  }
  
  def train(xdim: Int, ydim: Int, fdim: Int, iterations: Int, examples: RDD[SV], seed: Option[Int] = None): SOM = {
    val xSigmaStep = (xdim - 0.01) / iterations
    val ySigmaStep = (ydim - 0.01) / iterations
    val sc = examples.context
    val normedExamples = examples.map { 
      ex => {
        val bv = spark2breeze(ex)
        (bv, norm(bv))
      }
    }.cache
    
    (0 until iterations).foldLeft(random(xdim, ydim, fdim, seed)) { (acc, it) =>
      val xSigma = xdim - (xSigmaStep * it)
      val ySigma = ydim - (ySigmaStep * it)
      val currentSOM = sc.broadcast(acc)
      
      val newState = normedExamples.aggregate(SomTrainingState.empty(xdim * ydim, fdim))(
        {case (state, (example: Vector[Double], n: Double)) => state.update(currentSOM.value.closest(example, Some(n)), example)}, 
        {case (s1, s2) => s1.combine(s2)}
      )
      
      currentSOM.unpersist
      
      step(xdim, ydim, fdim, newState, xSigma, ySigma)
    }
  }
}
