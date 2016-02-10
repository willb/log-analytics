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

  @inline protected def magnitude: SV => Double = {
    case SV(_, _, vs) => scala.math.sqrt(vs.foldLeft(0.0d){(acc, v) => acc + v * v})
  }
  
  @inline protected def dot(v1: SV, v2: SV): Double = {
    val SV(_, i1s, v1s) = v1
    val SV(_, i2s, v2s) = v2
    dotImpl((i1s zip v1s).toList, (i2s zip v2s).toList)
  }
  
  @tailrec final private def dotImpl(v1: List[Pair[Int, Double]], v2: List[Pair[Int, Double]], acc:Double = 0d): Double = (v1, v2) match {
    case ((i1,_)::e1s, (i2,_)::e2s) if i1 < i2 => dotImpl(e1s, v2, acc)
    case ((i1,_)::e1s, (i2,_)::e2s) if i1 > i2 => dotImpl(v1, e2s, acc)
    case ((i1,v1)::e1s, (i2,v2)::e2s) if i1 == i2 => dotImpl(e1s, e2s, acc + (v1 * v2))
    case (Nil, _) => acc
    case (_, Nil) => acc
  }
  
  @inline protected def cossim(v1: SV, v2: SV): Double = {
    val n = dot(v1, v2)
    val d = magnitude(v1) * magnitude(v2)
    math.min(1.0, math.max(-1.0, n / d))
  }
  
  def apply(v1: V, v2: V): Double = {
    1 - (2 * math.acos(cossim(sparsify(v1), sparsify(v2))) / math.Pi)
  }
}

object AngularSimilarity extends AngularSimilarity {}