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
 * Iterate over lines in a sequence of files, tagging the lines with the tag associated with the file.
 * Only one file will ever be open at any one time.
 * Effectively does the opposite to SequentialFileWriter
 *
 * @author Florian Verhein
 */
class SequentialFileIterator[T](files : Iterator[(T,java.io.File)],
                                callBackOnEnd : () => Unit = () => Unit) extends Iterator[(T,String)] {

  private var curr = nextFile
  private var line : Option[String] = None

  def nextFile = {
    if (files.hasNext) {
      val (tag,file) = files.next
      val in = new java.io.BufferedReader(new java.io.FileReader(file))
      Some(tag,in)
    }
    else None
  }

  def hasNext = {
    if (!curr.isEmpty) {
      if (!line.isEmpty) {
        true
      }
      else { //read ahead
      var nLine = curr.get._2.readLine
        while(nLine == null && files.hasNext) {
          curr.get._2.close()
          curr = nextFile
          nLine = curr.get._2.readLine
        }
        line = if(nLine == null) None else Some(nLine)
        if (line.isEmpty)
        {
          callBackOnEnd()
          false
        } else true
      }
    } else {
      callBackOnEnd()
      false
    }
  }

  def next : (T,String) = {
    val r = (curr.get._1,line.get)
    line = None
    r
  }
}
