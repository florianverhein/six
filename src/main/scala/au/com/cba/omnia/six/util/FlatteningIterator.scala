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

package au.com.cba.omnia.six.util

/**
 * Flattens data in a streaming fashion into a sparse representation. Assumes correct ordering on the pivot.
 * For example, takes a stream of (iid,fid,value)s sorted by iid and turns it into a stream
 * of sparse rows List of (fid,value)'s with the same iid. By default, this implementation returns feature coordinates
 * in reverse of order seen for that instance, but clients can use maintainFidOrder to change this.
 *
 * @author Florian Verhein
 */
class FlateningIterator[X,C,V](delegate : Iterator[(X,C,V)], maintainFidOrder : Boolean = false) extends Iterator[(X,List[(C,V)])] {

  private var buff = None : Option[(X,C,V)]

  def hasNext = delegate.hasNext || !buff.isEmpty

  def next = {

    var current = List[(C,V)]()

    def update(n : (X,C,V)) = {
      current = (n._2,n._3) :: current
    }

    var n = buff.getOrElse(delegate.next)
    buff = None
    val currentId = n._1
    update(n)

    while (buff.isEmpty && delegate.hasNext)
    {
      n = delegate.next
      if (n._1 == currentId) {
        update(n)
      } else {
        buff = Some(n)
      }
    }
    if (maintainFidOrder)
      (currentId,current.reverse)
    else
      (currentId,current)
  }
}

object FlateningIterator {
  def apply[X,C,V](delegate : Iterator[(X,C,V)]) = new FlateningIterator(delegate)
}
