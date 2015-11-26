/*
 * stemFeatures.scala
 * author:  William Benton <willb@redhat.com>
 *
 * Copyright (c) 2015 Red Hat, Inc.
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

package com.redhat.et.logAnalysis.messages;

trait BasicStringCleaners {
  import BasicStringCleaners.{boringChars, spaces}
  def collapseWhitespace(s: String): String = spaces.replaceAllIn(s, " ")
  def stripPunctuation(s: String): String = boringChars.replaceAllIn(s, "")
  
}

object BasicStringCleaners {
  private [messages] val spaces = new scala.util.matching.Regex("[\\s]+")
  private [messages] val boringChars = new scala.util.matching.Regex("[^A-Za-z0-9-_]")
}
