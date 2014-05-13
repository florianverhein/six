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

import au.com.cba.omnia.six.util.FlateningIterator
import scala.reflect.ClassTag

/**
 * Tag for data
 */
sealed abstract class Action(action : String)
case object Train extends Action("T")
case object Validate extends Action("V")
case object Score extends Action("S")
case object Unassigned extends Action("U") // ATM for where data hasn't been replicated
case object TrainPerformance extends Action("TP") // Special marker for performance data -- TODO this is a bit of a hack (could remove once can have 3 outputs)
case object ScorePerformance extends Action("SP")

/**
 * This ordering is important -- it is relied upon for correct functioning.
 */
object Action {
  implicit def actionOrdering : Ordering[Action] = Ordering.by[Action,Int]( _ match {
    case Train => 1
    case Validate => 2
    case Score => 3
    case Unassigned => 4
    case TrainPerformance => 5
    case ScorePerformance => 6
  })
}

/**
 * Feature type
 */
sealed abstract class ValueType(vtype : String) //vtype is not required...
case object Numeric extends ValueType("N")
case object Factor extends ValueType("F")
case object Unknown extends ValueType("U")

object ValueType {
  implicit def valueTypeOrdering : Ordering[ValueType] = Ordering.by[ValueType,Int]( _ match {
    case Numeric => 1
    case Factor => 2
    case Unknown => 3
  })
  implicit def stringToValueType(s : String) : ValueType = s match {
    case "Numeric" => Numeric
    case "Factor" => Factor
    case "Unknown" => Unknown
  }
}


case class InputRecord[I,F,V](iid : I, fid : F, value : V)

object InputRecord {
  def fromTuple[I,F,V](t: (I, F, V)) : InputRecord[I,F,V] = (apply[I,F,V] _).tupled(t)
  def toTuple[I,F,V](r : InputRecord[I,F,V]) = (r.iid,r.fid,r.value)
}

//TODO change (bucket, fold, action) to parameterised type C (coord)
//In some cases, will be a collection. Also, will add config to it too. In future, will allow these concepts to be independent or linked.
case class SparseRecord[I,L,F,V](cid : Option[Int], bucket : Int, fold : Int, action : Action, iid : I, label : L, fid : F, value : V, valuesLength : Option[F] = None) {
  def toTuple = (cid,bucket,fold,action,iid,label,fid,value,valuesLength)
}

case class SparseFlatRecord[I,L,F,V](cid : Option[Int], bucket : Int, fold : Int, action : Action, iid : I, label : L, values : List[(F,V)], valuesLength : Option[F] = None)

case class DenseFlatRecord[I,L,V](cid : Option[Int], bucket : Int, fold : Int, action : Action, iid : I, label : L, values : Array[V])

/**
 * Convert between representations
 */
trait RecordTransformations {

  /** Note: Take care that stream is sorted appropriately. E.g. By bucket, fold, action, iid */
  def flattenToSparse[I,L,F,V](stream : Iterator[SparseRecord[I,L,F,V]]) : Iterator[SparseFlatRecord[I,L,F,V]] = {
    FlateningIterator(
      stream.map(r => ((r.cid, r.bucket, r.fold, r.valuesLength, r.action, r.label, r.iid), r.fid, r.value))
    ).map{
      case ((cid,b,f,fib,a,l,iid),vals) => SparseFlatRecord(cid,b,f,a,iid,l,vals,fib)
    }
  }

  def densifyValues[V : ClassTag](values : List[(Int,V)], rowLength : Int, missing : V) : Array[V] =
    values.foldLeft(Array.fill(rowLength)(missing)){
      case (array,(fid,v)) =>
        array(fid) = v
        array
    }

  //TODO pass in valuesLength (efficiency improvement)
  def sparseToDense[I,L,V : ClassTag](missing : V)(stream : Iterator[SparseFlatRecord[I,L,Int,V]]) : Iterator[DenseFlatRecord[I,L,V]] = {
    stream.map( r => DenseFlatRecord(r.cid,r.bucket,r.fold,r.action,r.iid,r.label,densifyValues(r.values,r.valuesLength.get,missing)))
  }

  def flattenToDense[I,L,V : ClassTag](missing : V)(stream : Iterator[SparseRecord[I,L,Int,V]]) =
    (flattenToSparse[I,L,Int,V] _).andThen(sparseToDense(missing))(stream)

}
