/*
   Copyright 2014 Commonwealth Bank of Australia

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
 */

package au.com.cba.omnia.six

import com.twitter.scalding._
import cascading.flow.FlowDef
import com.twitter.scalding.TDsl._

/**
 * Common args, Input formats, etc.
 */
trait JobCommon {

  type I = String; type F = String; type V = String; type L = String

  def getMissingValues(args : Args) : Set[String] =
    args.getOrElse("missing-values",null) match {
      case null => Set[String]()
      case str => str.split(",").toSet
    }

  /** iid\01fid\01value */
  def fromOsv3[X : Manifest](src : String)(implicit flowDef : FlowDef, mode : Mode) = TypedOsv[(I,F,X)](src)

  /** iid|fid|type|value */
  def fromPsv4[X : Manifest](src : String)(implicit flowDef : FlowDef, mode : Mode) = TypedPsv[(I,F,String,X)](src).map(r => (r._1,r._2,r._4))

  def getFrom[X : Manifest](args : Args, what : String)(implicit flowDef : FlowDef, mode : Mode) : TypedPipe[(I,F,X)] = {
    val argVal = args(what)
    args.getOrElse(what+"-encoding","osv3") match {
      case "osv3" => fromOsv3[X](argVal)
      case "psv4" => fromPsv4[X](argVal)
      case x => sys.error("Unknown "+what+"-encoding: " + x)
    }
  }

  def getLabelPipe(args : Args)(implicit flowDef : FlowDef, mode : Mode) = getFrom[L](args,"labels")

  def getFeaturePipe(args : Args)(implicit flowDef : FlowDef, mode : Mode) = getFrom[F](args,"features")

}
