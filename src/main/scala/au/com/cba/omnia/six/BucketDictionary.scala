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

case class BucketDictionaryEntry[F](fid : F, valueType : ValueType, fDictIndex : Int, fBucketIndex : Int) {
  override def toString = (valueType,fDictIndex,fBucketIndex).toString
}

case class BucketInfo[F](numNumeric : Int, numFactor : Int, features : List[F]) {
  def numFeatures = numNumeric + numFactor
  override def toString = (numNumeric,numFactor).toString
}

case class BucketEntry[F](info : BucketInfo[F], dictionary : Map[F,BucketDictionaryEntry[F]])

/**
 * Maps features to buckets and value types. This mapping is represented in multiple ways for efficiency reasons
 * in specific steps of the six.
 */
case class BucketDictionary[B,F](byBucket : Map[B,BucketEntry[F]], byFeature : Map[F,Seq[B]])

/**
 * Bucket dictionary building, etc.
 *
 * @author Florian Verhein
 */
trait BucketDictionaryTools[B,F] {

  def makeBucketDictionary(dict : Map[F,Int], assignBucketsF : F => Iterable[B], valueType : ValueType = Unknown) : BucketDictionary[B,F] = {
    makeBucketDictionary(dict.map{case (f,i) => f -> (i,valueType)}, assignBucketsF)
  }

  /** Feature value types can vary. Bucket feature index in order of valueType, then original feature index. */
  def makeBucketDictionary(dict : Map[F,(Int,ValueType)], assignBucketsF : F => Iterable[B]) : BucketDictionary[B,F] = {
    val d1 : Map[B,Seq[(B,F,Int,ValueType)]] = dict.toSeq.flatMap {
      case (fid,(i,vt)) => assignBucketsF(fid).map(b => (b,fid,i,vt))
    }.groupBy( _._1)

    val d2 : Map[B,BucketEntry[F]] = d1.map {
      case (b,vals) =>
        val dict : Seq[(F,BucketDictionaryEntry[F])] = vals
          .sortBy{ case (b,fid,i,vt) => (vt,i) }
          .zipWithIndex
          .map { case ((b,fid,i,vt),bi) => (fid,BucketDictionaryEntry(fid,vt,i,bi)) }
        val counts = dict
          .map(x => x._2.valueType match { case Numeric => (1,0) case Factor => (0,1) case default => (0,0)})
          .reduce[(Int,Int)]{ case ((n1,f1),(n2,f2)) => (n1+n2,f1+f2)}
        val feats = dict
          .sortBy(_._2.fBucketIndex).map(_._1).toList
        b -> BucketEntry(BucketInfo(counts._1,counts._2,feats),dict.toMap)
    }

    val fToB = d2.flatMap {
      case (b,be) => be.info.features.map(f => (f,b))
    }.groupBy(_._1).map {
      case (f,vals) => (f -> vals.map(_._2).toSeq)
    }

    BucketDictionary[B,F](d2,fToB)
  }
}
