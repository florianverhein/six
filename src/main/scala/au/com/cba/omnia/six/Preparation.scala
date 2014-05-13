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



trait InputPreparation[C] {

  def isBad(str : String) = {
    str == null || !str.find(c => c < ' ').isEmpty
  }

  def isMissing[V](missingValues : Set[V])(x : V) : Boolean = {
    missingValues.contains(x)
  }

  def guessValueType(value : String) = {
    scala.util.Try(value.toDouble).toOption match {
      case Some(_) => Numeric
      case default => Factor
    }
  }

  def hash(x : C) = Math.abs(x.hashCode)

  def hashSample(k : Int)(x : C) : Boolean =
    k == 1 || hash(x) % k == 0

  def assignFold(k : Int)(x : C) : Int =
    Math.abs(1777*x.hashCode()) % k

}


/** Given fold assignment, replicate and assign Actions using a strategy */
object FoldingFunctions {

  type Fold = Int

  /** Standard pick one fold to score cross validation */
  def crossValidation(folds : Fold)(fold : Fold) = {
    for {
      f <- Range(0,folds)
    }
    yield {
      val action = if (f == fold) Score else Train
      (f,action)
    }
  }

  /** One fold is scoring, remainder is test */
  def assignScoreFold(folds : Fold, scoreFold : Fold)(fold : Fold) : Iterable[(Fold,Action)] = {
      val action = if (scoreFold == fold) Score else Train
      (scoreFold,action) :: Nil
    }

}

/** Assign features to potentially multiple buckets */
object BucketingFunctions {

  import scala.util.Random

  type B = Int

  def distributeHashRandomlySequentialDuplication[F](buckets : B, duplication : B)(fid : F) : Iterable[B] = {
    val h = Math.abs(3571*fid.hashCode)
    duplication match {
      case d if (d <= 1) => h % buckets :: Nil
      case d if (d >= buckets) => Range(0,buckets)
      case default =>
        Range(0,duplication) map ( x => (x + h) % buckets)
    }
  }

  def distributeDeterministicRandomly[F](buckets : B, duplication : B)(fid: F) : Iterable[B] = {
    val r = new Random(fid.hashCode)
    (0 until buckets).map(b => r.nextInt).zipWithIndex.sortBy(_._1).map(_._2).take(duplication)
  }

}

/*

trait EnsemblePreparation[C] {


  //TODO make this more random, rather than have sequential overlap
  def assignBuckets(b : Int, d : Int)(x : C) : Iterable[Int] = {

    val h = Math.abs(3571*x.hashCode)

    d match {
      case d if (d <= 1) => h % b :: Nil
      case d if (d >= b) => Range(0,b)
      case default =>
        Range(0,d) map ( x => (x + h) % b)
    }
  }

  /** Replicates over buckets according to fid and bucketing function (simple duplication)
    * Replicates over folds as follows:
    * - if the instance is assigned to fold i, then
    *   - it's in the scoring set for fold i
    *   - it's in the training set for all other folds
    */
  def splitAndReplicate(
    bucketF : C => Iterable[Int],
    folds : Int)(fid : C, fold : Int) = {

    for {
      f <- Range(0,folds)
      b <- bucketF(fid)
    }
    yield {
      val action = if (f == fold) Score else Train
      (b,f,action)
    }
  }
}
 */
