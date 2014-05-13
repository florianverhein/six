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

import scala.sys.process._
import java.io.File

/**
 * Set of utilities for running processes, local file system handling, etc.
 * (Basically, factor out the ugly stuff into this)
 */
trait LocalTooling {

  def writeToLocal(file : java.io.File, content : String) = {
    val out = new java.io.BufferedWriter(new java.io.FileWriter(file))
    try {
      out.append(content)
    } finally {
      out.close
    }
  }

  def getTempLocalFile(localDir: String, prefix : String, suffix : String = null) =
    java.io.File.createTempFile(prefix,suffix,new java.io.File(localDir))

  def handleError(msg : String, t : Option[Throwable] = None, failOK : Boolean) = {
    //sys.error(msg) //This allows things to hang silently
    //TODO debug above
    // likely the runtime exception gets silently caught somewhere and causes the thread
    // to terminate this task, while other threads hang waiting on it via the pipes.
    println(msg)
    t.map(_.printStackTrace())
    if (!failOK) {
      println("Forcefully exiting JVM")
      //TODO OK for now but more robust error handling preferred.
      // This tears down JVM allowing job to fail on cluster,
      // which is exactly what we ultimately want in these cases -- it's just not a nice way of achieving it.
      sys.exit(1)
    } else {
      println("IGNORING ERROR AND CONTINUING")
    }
  }

  def runProcess(cmd : List[String], cwd : Option[File] = None, failOK : Boolean = false) = {
    try{
      println("PROCESS: cwd="+cwd+" cmd="+cmd.mkString(" "))
      Process(cmd, cwd).! match {
        case 0 =>
          println("PROCESS COMPLETED: "+cmd.mkString(" "))
          ()
        case e =>
          handleError("PROCESS FAILED: Exit code " + e + " from "+cmd.mkString(" "), failOK = failOK)
      }
    } catch { //This happens if there is a problem before the process can be started.
      case t : Throwable =>
        handleError("PROCESS FAILED: Exception: " + t, Some(t), failOK)
    }
  }

  def uploadToHdfs(hadoopCmd : String = "hadoop")(source: String, dest: String): Unit = {
    Option(new java.io.File(dest).getParent)
      .map{ case dir =>
        val mkdir = hadoopCmd :: "fs" :: "-mkdir" :: "-p" :: dir :: Nil
        runProcess(mkdir, failOK = true)
    }
    val cmd = hadoopCmd :: "fs" :: "-copyFromLocal" :: source :: dest :: Nil
    runProcess(cmd, failOK = true)
  }

  def chmod(file : java.io.File, args : String = "+x") = {
    val cmd = List("chmod", args, file.getAbsolutePath)
    runProcess(cmd)
  }

  def exec(file : java.io.File, args: List[String], cwd : Option[File] = None) = {
    chmod(file)
    val cmd = file.getAbsolutePath :: args
    runProcess(cmd,cwd)
  }
}
