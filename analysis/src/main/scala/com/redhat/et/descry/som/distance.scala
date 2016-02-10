/*
 * distance.scala
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

package com.redhat.et.descry.som;

import org.apache.spark.mllib.linalg.{Vector=>V, DenseVector=>DV, SparseVector=>SV}

class AngularSimilarity extends Function2[V, V, Double] {
  @inline protected def sparsify: V => SV = {
    case d@DV(_) => d.toSparse
    case s@SV(_, _, _) => s
  } 

  @inline protected def magnitude: SV => Double = {
    case SV(_, _, vs) => scala.math.sqrt(vs.foldLeft(0.0d){(acc, v) => acc + v * v})
  }
  
  @inline protected def dot(v1: SV, v2: SV): Double = {
    ((v1 match { case SV(_, is, _) => is.toSet })
      .intersect(v2 match { case SV(_, is, _) => is.toSet }))
      .toArray
      .map { i => v1(i) * v2(i) }
      .sum
  }
  
  @inline protected def cossim(v1: SV, v2: SV): Double = {
    val num = dot(sparsify(v1), sparsify(v2))
    val den = magnitude(sparsify(v1)) * magnitude(sparsify(v2))
    num / den
  }
  
  def apply(v1: V, v2: V): Double = {
    val num = dot(sparsify(v1), sparsify(v2))
    val den = magnitude(sparsify(v1)) * magnitude(sparsify(v2))
    1 - (2 * math.acos(num / den) / math.Pi)
  }
}

object AngularSimilarity extends AngularSimilarity {}