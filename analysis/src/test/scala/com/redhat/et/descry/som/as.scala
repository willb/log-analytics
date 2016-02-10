/*
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

import org.scalatest._
import org.apache.spark.mllib.linalg.{Vector=>V, DenseVector=>DV, SparseVector=>SV}

class ASSpec extends FlatSpec with Matchers {
  it should "return expected results for some toy examples" in {
    val as = com.redhat.et.descry.som.AngularSimilarity
    val v1 = new SV(12, Array(1, 3, 5), Array(1.0, 1.0, 1.0))
    val v2 = new SV(12, Array(2, 4, 6), Array(1.0, 1.0, 1.0))
    val v3 = new SV(12, Array(2, 4, 5), Array(1.0, 1.0, 1.0))
    assert(as(v1, v1) == 1.0)
    assert(as(v1, v2) == 0.0)
    assert(as(v1, v3) == 0.21634689593878542)
    assert(as(v2, v3) == 0.4645590543975401)
  }
}