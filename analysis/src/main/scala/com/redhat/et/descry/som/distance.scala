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
import scala.annotation.tailrec

class AngularSimilarity extends Function2[V, V, Double] {
  @inline def sparsify: V => SV = {
    case d@DV(_) => d.toSparse
    case s@SV(_, _, _) => s
  } 

  @inline protected def magnitude(sv: SV): Double = {
    val SV(_, _, vs) = sv
    scala.math.sqrt(vs.foldLeft(0.0d)((acc, v) => acc + (v*v)))
  }
  
  @inline private final def dot(v1: SV, v2: SV): Double = {
    val SV(_, i1s, v1s) = v1
    val SV(_, i2s, v2s) = v2
    
    var acc: Double = 0.0
    var idx1: Int = 0
    var idx2: Int = 0
    val l1 = i1s.length
    val l2 = i2s.length
    
    while(idx1 < l1 && idx2 < l2) {
      if(i1s(idx1) > i2s(idx2)) {
        idx2 = idx2 + 1
      } else if (i1s(idx1) < i2s(idx2)) {
        idx1 = idx1 + 1
      } else {
        acc = acc + (v1s(idx1) * v2s(idx2))
        idx1 = idx1 + 1
        idx2 = idx2 + 1
      }
    }
    
    acc
  }
  
  @inline protected def cossim(v1: SV, v2: SV, mv1: Option[Double] = None, mv2: Option[Double] = None): Double = {
    val n = dot(v1, v2)
    val d = mv1.getOrElse(magnitude(v1)) * mv2.getOrElse(magnitude(v2))
    math.min(1.0, math.max(-1.0, n / d))
  }
  
  def apply(v1: V, v2: V): Double = {
    1 - (2 * math.acos(cossim(sparsify(v1), sparsify(v2))) / math.Pi)
  }
}

object AngularSimilarity extends AngularSimilarity {}