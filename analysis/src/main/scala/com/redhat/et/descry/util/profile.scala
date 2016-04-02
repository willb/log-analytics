/*
 * profile.scala
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

import com.redhat.et.silex.app.AppCommon

trait ProfileFixture extends Function1[org.apache.spark.SparkContext, Unit] {}

object Profile extends AppCommon {
  val fixtures = Map(
    "com.redhat.et.descry.som.Profile" -> com.redhat.et.descry.som.Profile, 
    "com.redhat.et.descry.som.ProfileSparse" -> com.redhat.et.descry.som.ProfileSparse
  )
  
  override def appName = "descry-profile"
  override def appMain(args: Array[String]) {
    args.foreach {which => fixtures.get(which).map { fixture => fixture.apply(context) } }
  }
}