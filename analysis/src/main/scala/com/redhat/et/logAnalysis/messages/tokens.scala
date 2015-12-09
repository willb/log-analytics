/*
 * tokens.scala
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
  import BasicStringCleaners._
  private [messages] val collapseWhitespace: String => String = replace(spaces, " ")
  private [messages] val stripPunctuation: String => String = 
    replace(leadingPunctuation, " ") compose 
    replace(trailingPunctuation, " ") compose 
    replace(rejectedIntratokenPunctuation, "")
  
  def tokens(s: String, post: String=>String = identity[String]): Seq[String] = 
    collapseWhitespace(s)
      .split(" ")
      .map(s => post(stripPunctuation(s)))
      .collect { case token @ oneletter(_) => token } 
}

object BasicStringCleaners {
  private [messages] val spaces = new scala.util.matching.Regex("[\\s]+")
  private [messages] val oneletter = new scala.util.matching.Regex(".*([A-Za-z_-]).*")
  private [messages] val rejectedIntratokenPunctuation = new scala.util.matching.Regex("[^A-Za-z0-9-_./:@]")
  private [messages] val leadingPunctuation = new scala.util.matching.Regex("\\s[^A-Za-z0-9-_/]+")
  private [messages] val trailingPunctuation = new scala.util.matching.Regex("[^A-Za-z0-9-_/]+\\s")
  private [messages] def replace(r: scala.util.matching.Regex, s: String) = { (orig:String) => r.replaceAllIn(orig, s) }
}
