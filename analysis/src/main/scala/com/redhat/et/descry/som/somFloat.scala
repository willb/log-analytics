/*
 * somFloat.scala
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

object FloatHood {
  private [this] def gaussian(distance: Float, sigma: Float): Float = math.exp(- (distance * distance) / (sigma * sigma)).toFloat
  
  /** Returns a <tt>dim</tt>-element vector of the values for the one-dimensional Gaussian neighborhood function, centered at <tt>c</tt> */
  def vec(c: Int, dim: Int, sigma: Float): DenseVector[Float] = {
    // weights for each distance
    val weights = (0 to math.max(c, math.abs(dim - c))).map { x => gaussian(x.toFloat, sigma) }
    DenseVector((0 until dim).map { x => weights(math.abs(x - c))}.toArray)
  }
  
  /** Returns a <tt>xdim</tt>*<tt>ydim</tt>-element matrix of the values for the two-dimensional Gaussian neighborhood function, centered at <tt>xc</tt>, <tt>yc</tt> */
  def mat(xc: Int, xdim: Int, xsigma: Float, yc: Int, ydim: Int, ysigma: Float): DenseMatrix[Float] = {
    vec(yc, ydim, ysigma) * vec(xc, xdim, xsigma).t
  }
}

class FloatSOM(val xdim: Int, val ydim: Int, val fdim: Int, _entries: DenseVector[DenseVector[Float]], val worstSimOpt: Option[Float] = None) extends Serializable {
  import breeze.numerics._
  
  val entries = _entries.toArray
  
  val norms = entries.zip(entries.map(norm(_).toFloat)).zipWithIndex
  
  def worstSim(orElse: Float = Float.PositiveInfinity) = worstSimOpt.getOrElse(orElse)
  
  def closest(example: Vector[Float], exampleNorm: Option[Float] = None): Int = {
    (closestWithSimilarity(example, exampleNorm))._1
  }
  
  /** Return the index of the most similar vector in the map to the supplied example, along with its similarity */
  def closestWithSimilarity(example: Vector[Float], exampleNorm: Option[Float] = None): (Int, Float) = {
    val vn = exampleNorm.getOrElse(norm(example).toFloat)
    val ce = norms(0)
    val initialCandidate = (0, math.min(1.0, math.max(-1.0, (example dot ce._1._1) / (ce._1._2 * vn))).toFloat)
    
    norms.foldLeft(initialCandidate) { 
      case(answer: (Int, Float), ((e: Vector[Float], en: Float), i: Int)) => {
        val candidate = (i, math.min(1.0f, math.max(-1.0f, (example dot e).toFloat / (en * vn))))
        if (answer._2 > candidate._2) answer else candidate
      }
    }
  }
}

object FloatSOM {
  import breeze.numerics._
  import org.apache.spark.rdd.RDD
  import org.apache.spark.mllib.linalg.{Vector=>SV, DenseVector=>SDV, SparseVector=>SSV}
  
  private [som] case class SomTrainingState(counts: Array[Int], weights: Array[DenseVector[Float]], worst: Float) {
    /* destructively updates this state with a new example */
    def update(index: Int, example: Vector[Float]): SomTrainingState = {
      counts(index) = counts(index) + 1
      weights(index) = weights(index) + example
      this
    }
    
    /* destructively updates the arrays in this state with a new example; maybe creates a new wrapper to hold a new worst similarity */
    def update(indexAndSimilarity: (Int, Float), example: Vector[Float]): SomTrainingState = {
      val (index, similarity) = indexAndSimilarity
      val ret = (if (worst < similarity) this else this.copy(worst=similarity))
      ret.update(index, example)
    }
    
    /* destructively merges other into this */
    def combine(other: SomTrainingState) = { 
      (0 until counts.length) foreach { index =>
        this.counts(index) = this.counts(index) + other.counts(index)
        this.weights(index) = this.weights(index) + other.weights(index)
      }
      
      if(this.worst < other.worst) this else this.copy(worst=other.worst)
    } 
  }
  
  private [som] object SomTrainingState {
    def empty(dim: Int, fdim: Int): SomTrainingState = SomTrainingState(Array.fill(dim)(0), Array.fill(dim)(DenseVector.zeros[Float](fdim)), Float.PositiveInfinity)
  }
  
  /** initialize a self-organizing map with random weights */
  def random(xdim: Int, ydim: Int, fdim: Int, seed: Option[Int] = None): FloatSOM = {
    // nb: could/should use breeze PRNGs?
    val rng = seed.map { s => new scala.util.Random(s) }.getOrElse(new scala.util.Random())
    val randomMap = DenseVector.fill[DenseVector[Float]](xdim * ydim)(DenseVector.fill[Float](fdim)(rng.nextFloat()))

    new FloatSOM(xdim, ydim, fdim, randomMap)
  }
  
  /** Create a new SOM instance with the results of the training state */
  private [som] def step(xdim: Int, ydim: Int, fdim: Int, state: SomTrainingState, xsigma: Float, ysigma: Float, lastState: FloatSOM) = {
    var weights = DenseVector.fill[DenseVector[Float]](xdim * ydim)(DenseVector.zeros[Float](fdim))
    val seenWeights = DenseVector[DenseVector[Float]](state.weights)
    var neighborhoods = DenseVector.zeros[Float](xdim * ydim)
    
    (0 until xdim * ydim).foreach { idx =>
      val (xc, yc) = (idx / ydim, idx % ydim)
      val counts = state.counts(idx).toFloat
      val hood = FloatHood.mat(xc, xdim, xsigma, yc, ydim, ysigma).reshape(xdim * ydim, 1).toDenseVector
      neighborhoods = neighborhoods :+ (hood * counts)
      val update = DenseVector(hood.toArray.map { hoodDistance => state.weights(idx) * hoodDistance})
      weights = weights :+ update
    }
    
    val newWeights = DenseVector((weights.values.iterator.zip(lastState.entries.iterator)).zip(neighborhoods.values.iterator).map { 
      case ((vd, od), d) if d == 0.0 => od
      case ((vd, _), d) => vd / d 
    }.toArray)
    new FloatSOM(xdim, ydim, fdim, newWeights, Some(state.worst))
  }
  
  @inline private [som] def spark2breeze(vec: SV): Vector[Float] = {
    vec match {
      case sv: SSV => {
        val vb = new VectorBuilder[Float](sv.size)
        (sv.indices zip sv.values).foreach { case (idx: Int, v: Double) =>
          vb.add(idx, v.toFloat)
        }
        vb.toSparseVector()
      }
      
      case dv: SDV => {
        DenseVector(dv.toArray.map{ _.toFloat})
      }
    }
  }
  
  def train(xdim: Int, ydim: Int, fdim: Int, iterations: Int, examples: RDD[SV], seed: Option[Int] = None, sigmaScale: Float = 0.95f, minSigma: Float = 1, hook: (Int, FloatSOM) => Unit = { case (_,_) => }): FloatSOM = {
    val xSigmaStep = ((xdim * sigmaScale) - minSigma) / iterations
    val ySigmaStep = ((ydim * sigmaScale) - minSigma) / iterations
    val sc = examples.context
    val normedExamples = examples.map { 
      ex => {
        val bv = spark2breeze(ex)
        (bv, norm(bv).toFloat)
      }
    }.cache
    
    val startState = random(xdim, ydim, fdim, seed)
    
    hook(0, startState)
    
    (0 until iterations).foldLeft(startState) { (acc, it) =>
      val xSigma = (xdim * sigmaScale) - (xSigmaStep * it)
      val ySigma = (ydim * sigmaScale) - (ySigmaStep * it)
      val currentSOM = sc.broadcast(acc)
      
      val newState = normedExamples.aggregate(SomTrainingState.empty(xdim * ydim, fdim))(
        {case (state, (example: Vector[Float], n: Float)) => 
          state.update(currentSOM.value.closestWithSimilarity(example, Some(n)), example)}, 
        {case (s1, s2) => s1.combine(s2)}
      )
      
      currentSOM.unpersist
      
      val nextSOM = step(xdim, ydim, fdim, newState, xSigma, ySigma, acc)
      hook(it + 1, nextSOM)
      nextSOM
    }
  }
}
