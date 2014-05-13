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
import com.twitter.scalding.typed.ValuePipe

//import scalaz._, Scalaz._
import com.twitter.scalding._

import org.specs2._

trait SampleData {

  lazy val numericFeatures = List(
    "i0|F1|10",
    "i0|F2|20",
    "i0|F3|30",
    "i0|F4|40",
    "i0|F5|50",
    "i1|F1|10",
    "i1|F2|11",
    "i1|F3|20",
    "i2|F2|1",
    "i2|F3|2",
    "i3|F1|11",
    "i3|F2|12",
    "i3|F4|13",
    "i3|F5|14",
    "i4|F1|100",
    "i4|F2|20",
    "i4|F3|21",
    "i4|F4|200",
    "i4|F5|1",
    "i5|F2|10",
    "i5|F5|11",
    "i6|F2|30",
    "i6|F3|null"
  )

  lazy val factorFeatures = List(
    "i0|F6|6A",
    "i0|F7|7B",
    "i0|F8|8A",
    "i1|F6|6B",
    "i1|F7|7B",
    "i1|F8|8B",
    "i2|F7|7C",
    "i3|F6|6C",
    "i3|F7|7B",
    "i3|F8|8A",
    "i4|F7|7C",
    "i6|F6|6A"
  )

  lazy val features = numericFeatures ++ factorFeatures

  lazy val labels = List(
    "i0|Label|0",
    "i1|Label|1",
    "i2|Label|0",
    "i3|Label|0",
    "i4|Label|1",
    "i5|Label|0",
    "i6|Label|0"
  )

  lazy val numericDictionary = List(
    ("F1",0,Numeric,4,0),
    ("F2",1,Numeric,7,0),
    ("F3",2,Numeric,4,0),
    ("F4",3,Numeric,3,0),
    ("F5",4,Numeric,4,0)
  )

  lazy val factorDictionary = List(
    ("F6",0,Factor,0,4),
    ("F7",1,Factor,0,5),
    ("F8",2,Factor,0,3)
  )

  lazy val dictionary = numericDictionary ++ factorDictionary.map{
    case (fid,i,vt,n,f) => (fid,i+numericDictionary.size,vt,n,f)
  }

  lazy val configs = List(
    "1|blah",
    "2|blah"
  )

}

class CreateDictionaryTest extends Specification with SampleData {

  def is = s2"""
    Ensure dictionary created correctly
      test creation   $testDict
 """"

  def testDict = {

    var res = false
    JobTest(new CreateDictionary(_))
      .arg("features", "fakeFeatures")
      .arg("missing-values","null,Null")
      .arg("output", "fakeOutput")
      .source(TypedOsv[(String, String, String)]("fakeFeatures"), features.map(l => {
        val parts = l.split("\\|")
        (parts(0), parts(1), parts(2))
      }).toIterable)
      .sink[(String,Int,ValueType,Int,Int)](TypedPsv[(String,Int,ValueType,Int,Int)](s"fakeOutput"))(output => {
        //println(output.mkString("\n"))
        //println(dictionary.mkString("\n"))
        res = output == dictionary
      })
      .run.finish
    res
  }
}

class EnsemblePrepTest extends Specification with SampleData {
  def is = s2"""

    EnsemblePrep sample test
      test 1  $t1
  """

  def t1 = {

    var res = false
    JobTest(new Six(_))
      .arg("dictionary","fakeDictionary")
      .arg("labels", "fakeLabels")
      .arg("features", "fakeFeatures")
      .arg("missing-values","null,Null")
      .arg("output", "fakeOutput")
      //.arg("output-dict", "fakeOutputDict")
      .arg("folds", "2")
      .arg("buckets", "2")
      .arg("bucket-replication", "1")

      .arg("output-models-prefix","~/temp/")
      .arg("output-perf-prefix","~/temp/")

      // --- Various modes ---

      //.arg("mode","prep-sparse")
      //.arg("mode","prep-dense")

      //.arg("mode","model1")
      //.arg("script-1","src/main/R/piped_model_null.R")

      .arg("mode","model2")
      //.arg("script-1","src/main/R/piped_model_null.R")
      //.arg("script-2","src/main/R/piped_model_null.R")

      .arg("hdfs-script-dir","src/main/R/")
      //.arg("hdfs-script-name-1","piped_model_tree_and_glm.R")
      //.arg("hdfs-script-name-2","piped_model_tree_and_glm.R")
      .arg("hdfs-script-name-1","piped_model_null.R")
      .arg("hdfs-script-name-2","piped_model_null.R")

      .arg("hdfs-configs", "fakeHdfsConfigs")

      .arg("node-hadoop-cmd","echo") //Do nothing
      .arg("dont-split-output","blah")

      .source(TypedPsv[(String, Int, String, Int, Int)]("fakeDictionary"), dictionary.map{
        case (fid,i,vt,n,f) => (fid,i,vt.toString,n,f) })
      .source(TypedOsv[(String, String, String)]("fakeFeatures"), features.map(l => {
        val parts = l.split("\\|")
        (parts(0), parts(1), parts(2))
      }).toIterable)
      .source(TypedOsv[(String, String, String)]("fakeLabels"), labels.map(l => {
        val parts = l.split("\\|")
        (parts(0), parts(1), parts(2)) //parts(2).toDouble)
      }).toIterable)

      .source(TypedPsv[(Int, String)]("fakeHdfsConfigs"), configs.map(l => {
      val parts = l.split("\\|")
      (parts(0).toInt, parts(1))
      }).toIterable)
      /*.sink[String](TypedPsv[String](s"fakeOutputDict"))(outputDict => {
        println(outputDict)
      })*/
      //.sink[(Int, Int, String, String, String, Int, String, Option[Int])](TypedPsv[(Int,Int, Action, String, String, Int, String, Option[Int])](s"fakeOutput"))(output => { //raw
        .sink[((Option[Int],Int, Int),(Action, String, String, String))](TypedPsv[((Option[Int],Int,Int), (Action, String, String, String))](s"fakeOutput"))(output => { //dense, sparse
         println(output.toList.mkString("\n"))
         res = output.size == 36
         //TODO check output content given a specific modelling script
      })
    .run.finish
    //.runHadoop.finish
    res
  }


}
