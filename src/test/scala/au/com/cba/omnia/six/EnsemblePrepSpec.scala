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

import org.specs2._
import org.specs2.matcher.ThrownExpectations

class BucketDictionarySpec extends Specification
    with SampleData with BucketDictionaryTools[Int,String] { def is = s2"""

    Ensure bucketed dictionary created correctly
      check correct for small sample       $d1
      check by bucket correct for sample   $testByBucket
      check by feature correct for sample  $testByFeature

"""
  //Basic
  def bucket1Func(n : Int)(s : String) : Iterable[Int] = {
    val i = s.tail.toInt
    i % n :: Nil
  }

  def bucket2Func(n : Int)(s : String) : Iterable[Int] = {
    val i = s.tail.toInt
    i % n :: (i + 1) %n :: Nil
  }

  def mkDict(n : Int) = {
    val g = for {
      i <- Range(0,n)
    } yield ("F"+i.toString,i*10)
    g.toMap
  }

  val sample = mkDict(10)
  val bucketF = bucket2Func(5) _


  def d1 = {
    val bd = makeBucketDictionary(sample,bucketF)
    bd.byBucket.find{case (k,v) => v.dictionary.size != 4}.isEmpty //expect 2 in each bucket + 2 more from replication of others
  }

  val expected = BucketDictionary(
    Map(
      1 -> BucketEntry(
        BucketInfo(3,1,List("F1","F3","F5","F7")),
        Map(
        "F1" -> BucketDictionaryEntry("F1",Numeric,0,0),
        "F3" -> BucketDictionaryEntry("F3",Numeric,2,1),
        "F5" -> BucketDictionaryEntry("F5",Numeric,4,2),
        "F7" -> BucketDictionaryEntry("F7",Factor,6,3)
      )),
      0 -> BucketEntry(
        BucketInfo(2,2,List("F2","F4","F6","F8")),
        Map(
        "F2" -> BucketDictionaryEntry("F2",Numeric,1,0),
        "F4" -> BucketDictionaryEntry("F4",Numeric,3,1),
        "F6" -> BucketDictionaryEntry("F6",Factor,5,2),
        "F8" -> BucketDictionaryEntry("F8",Factor,7,3)
      ))
    ),
    Map(
      "F1" -> Seq(1),
      "F2" -> Seq(0),
      "F3" -> Seq(1),
      "F4" -> Seq(0),
      "F5" -> Seq(1),
      "F6" -> Seq(0),
      "F7" -> Seq(1),
      "F8" -> Seq(0)
    )
  )

  def testByBucket = {
    val dict = dictionary.map(x => x._1 -> (x._2,x._3)).toMap
    //println(dict)
    val bdict = makeBucketDictionary(dict, bucket1Func(2) _)
    //println(bdict.byBucket.mkString("\n"))
    bdict.byBucket == expected.byBucket
  }

  def testByFeature = {
    val dict = dictionary.map(x => x._1 -> (x._2,x._3)).toMap
    val bdict = makeBucketDictionary(dict, bucket1Func(2) _)
    //println(bdict.byFeature.mkString(","))
    //println(expected.byFeature.mkString(","))
    bdict.byFeature == expected.byFeature
  }
}



class InputPreparationSpec extends Specification {
  def is = s2"""
    Check bucketing function
      Does duplication   $simpleDuplicationCheck
  """

  def simpleDuplicationCheck = {
    val l = for {
      i <- Range(0,10)
    } yield (i -> BucketingFunctions.distributeHashRandomlySequentialDuplication(5,2)(i.toString))
    l.find(x => x._2.size != 2).isEmpty
  }
}
