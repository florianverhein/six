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
import com.twitter.scalding.TDsl._
import com.twitter.scalding.typed.Grouped
import cascading.flow.FlowDef

//For serialisation of dictionary (ideas...)
//import org.apache.hadoop.io.{BytesWritable, Text, Writable}
//import com.twitter.scalding.WritableSequenceFile
//import com.twitter.bijection.scrooge.CompactScalaCodec

//TODO case class for DictionaryEntry instead of tuple

/**
 * A simple dictionary of features, also summarising their types.
 *
 * @author Florian Verhein
 */
trait Dictionary extends InputPreparation[String] {

  def selectValueType(nnum : Int, nfac : Int) =
    if (nfac == 0) Numeric else Factor

  def computeDictionary(features : TypedPipe[InputRecord[String,String,String]], missingValues : Set[String]) : TypedPipe[(String,Int,ValueType,Int,Int)] = {
    features
      .filter{ case r =>
        !isMissing(missingValues)(r.value) && !isBad(r.value)
      }
      .map{ case r =>
        guessValueType(r.value) match {
          case Numeric => (r.fid,1,0)
          case Factor  => (r.fid,0,1)
        }
      }
      .groupBy(_._1)
      .mapValues{case (_,n,f) => (n,f)}
      .reduce[(Int,Int)]{ case ((n1,f1),(n2,f2)) => (n1+n2,f1+f2)}
      .groupAll
      .sortBy(_._1) //Grouped[Unit,(F,(Int,Int))]
      .mapValueStream(x => x.zipWithIndex)
      .mapValues{ case ((fid,(nnum,nfac)),index) =>
        (fid,index,selectValueType(nnum, nfac),nnum,nfac)
      }
      .toTypedPipe.map(_._2)
  }
}


class CreateDictionary(args : Args) extends Job(args)
   with JobCommon
   with Dictionary {

  val features = getFeaturePipe(args)
    .map(InputRecord.fromTuple[I,F,V])
  val output = args("output")
  val missingValues = getMissingValues(args)

  val dictionary = computeDictionary(features,missingValues)

  dictionary.write(TypedPsv[(F,Int,ValueType,Int,Int)](output))

}
