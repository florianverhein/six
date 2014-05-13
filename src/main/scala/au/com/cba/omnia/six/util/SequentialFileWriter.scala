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
 * Writes data to multiple files files sequentially where the destination is determined by a tag.
 * Assumes data is tagged in blocks and that the tags in the data have the same order as the tags in files.
 * Note: Data with unknown tags is handled as follows:
 * - (tag,file)s will be dropped until a match is found
 * - if none is found, line will be dropped.
 * Properties:
 * - Only one file will ever be open at any time.
 * - The Iterator with data is always completely consumed.
 * - All files will be opened and closed even if there is no data for them.
 * These are important for use with named pipes having processes hanging on them.
 *
 * @author Florian Verhein
 */
class SequentialFileWriter[T](files : Iterator[(T,java.io.File)]) {

  def buildWriter(f : java.io.File) = {
    try{
      val p = new java.io.PrintWriter(new java.io.BufferedWriter(new java.io.FileWriter(f)))
      p
    } catch {
      case t : Throwable => t.printStackTrace
        throw t
    }
  }

  def run(data : Iterator[(T,String)]) : Unit = {

    def next = if (files.hasNext) {
      val n = files.next
      Some((n._1,buildWriter(n._2)))
    } else None

    var curr = next

    def handleLine(tag : T, line : String) = {
      curr match {
        case Some((currTag,currOut)) if (tag == currTag) =>
          currOut.println(line)
          false
        case Some((currTag,currOut)) =>
          currOut.flush
          currOut.close
          curr = next
          true
        case None =>
          //Exhausted all files. Line will be dropped
          false
      }
    }

    def handleRest = {
      curr match {
        case Some((_, out)) =>
          out.flush
          out.close
          curr = next
          true
        case None => false
      }
    }

    try {
      data.foreach {
        case (tag,line) =>
          while (handleLine(tag,line)) {}
      }
    } finally {
      while (handleRest) {}
    }
  }
}



