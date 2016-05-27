/*
 * imageUtils.scala
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

package com.redhat.et.descry.util

import java.awt.image.BufferedImage
import javax.imageio.ImageIO

import scala.util.Try

import breeze.linalg.{DenseVector => BDV}

object ImageCallbacks {
  type CoordinateHook = Int => (Int, Int)
  type ColorHook = BDV[Double] => Int
  
  def rowMajor(cols: Int)(idx: Int) = (idx / cols, idx % cols)
  def columnMajor(rows: Int)(idx: Int) = (idx / rows, idx % rows)
  
  def normalizedToGrayscale(dv: BDV[Double]): Int = {
    assert(dv.length == 1)
    ((dv(0) * 255).toInt << 16) | ((dv(0) * 255).toInt << 8) | (dv(0) * 255).toInt
  }
  
  def normalizedToGrayscaleInverse(dv: BDV[Double]): Int = {
    assert(dv.length == 1)
    val v = ((1.0 - dv(0)) * 255).toInt
    (v << 16) | (v << 8) | v
  }
  
  def vec2rgb(dv: BDV[Double]): Int = {
    assert(dv.length >= 3)
    ((dv(0) * 255).toInt << 16) | ((dv(1) * 255).toInt << 8) | (dv(2) * 255).toInt
  }
  
  def vec2argb(dv: BDV[Double]): Int = {
    assert(dv.length >= 4)
    ((dv(0) * 255).toInt << 24) | ((dv(1) * 255).toInt << 16) | ((dv(2) * 255).toInt << 8) | (dv(3) * 255).toInt
  }
}

object ImageWriter {  
  import ImageCallbacks._
  
  def write(xdim: Int, ydim: Int, vecs: Seq[BDV[Double]], file: String, kind: String = "PNG") {
    write(xdim, ydim, vecs, file, kind, columnMajor(ydim), vec2rgb _)
  }
  
  def write(xdim: Int, ydim: Int, vecs: Seq[BDV[Double]], file: String, kind: String, coordFunc: CoordinateHook, colFunc: ColorHook) {
    val image = new BufferedImage(xdim, ydim, BufferedImage.TYPE_INT_RGB)
    val ifile = new java.io.File(file)
    
    vecs.zipWithIndex.foreach { case (vec, idx) => 
      val color = colFunc(vec)
      val (x, y) = coordFunc(idx)
      image.setRGB(x, y, color)
    }
    
    javax.imageio.ImageIO.write(image, kind, ifile)
  }
}