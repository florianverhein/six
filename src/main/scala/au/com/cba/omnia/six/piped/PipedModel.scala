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

package au.com.cba.omnia.six.piped

import com.cba.omnia.piped.utils.NamedPipe
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import com.twitter.scalding.filecache.CachedFile
import java.io.File
import au.com.cba.omnia.six._
import scala.Some
import au.com.cba.omnia.six.util.{LocalTooling, SequentialFileWriter, SequentialFileIterator}


case class PipedModelArgs(layer : Int, numNumeric : Int, numFactor : Int, hdfsModelOutput: String, cachedLib : Option[CachedFile], config : Option[String], extraArgs : List[String] = Nil)

sealed abstract class ScriptSource
final case class StringSource(s: String) extends ScriptSource
final case class CacheSource(f : CachedFile) extends ScriptSource
final case class FileSource(f : File) extends ScriptSource

//TODO consider changing the interface from command line arguments to a json file placed on the local filesystem with path passes as a arg. much more flexible.

//TODO finish config handling

/**
 * Runs an executable modelling script using named pipes and a well defined interface.
 *
 *  1. Splits the stream of data (assumed to have training data followed by scoring data followed by optional performance data) into three named pipes.
 *     The performance data is simply passed through.
 *  2. Executes the script to train the model, score it and evaluate performance -- all on the training set.
 *     (The script reads from the training data pipe and writes to a pipe for the scores and another for the performance.
 *     The script also outputs a trained model to the local filesystem).
 *  3. Writes the model to hdfs
 *  4. Scores and evaluates the performance of the model on the scoring data set.
 *     (The script operates identically to the training step above except that instead of training it reads from the model file).
 *  5. The returned iterator contains the scores for the training set followed by those for the test set followed by performance data (new and existing).
 *     (The iterator reads from the pipes in turn)
 *
 *  Supports any script / executable accepting the following arguments:
 *    layer dataFile numNumeric numFactor modelFile train test perf [scoreFile] [perfFile] [libraries] [configFile] [...]
 *  where
 *    modelFile is treated as an input if !train, otherwise treated as output.
 *    scoreFile contains scores in same format as the dataFile.
 *
 *    @author Florian Verhein
 */
class PipedModel(script : ScriptSource, localDir: String, hadoopCmd : String) extends LocalTooling {

  def run(data : Iterator[(Action,String)], args : PipedModelArgs) : Iterator[(Action, String)] = {

    val cachedLib = args.cachedLib.map(_.path)

    val trainInput = NamedPipe.createTempPipe("train_input")
    val scoreInput = NamedPipe.createTempPipe("score_input")

    val trainModelOutput = getTempLocalFile(localDir, "train_model_output")

    val trainOutput = NamedPipe.createTempPipe("train_output")
    val scoreOutput = NamedPipe.createTempPipe("score_output")

    val trainPerfThrough = NamedPipe.createTempPipe("train_perf_through")
    val scorePerfThrough = NamedPipe.createTempPipe("score_perf_through")

    val trainPerfOutput = NamedPipe.createTempPipe("train_perf_output")
    val scorePerfOutput = NamedPipe.createTempPipe("score_perf_output")

    val allPipes = trainInput :: scoreInput :: trainOutput :: scoreOutput :: trainPerfThrough :: scorePerfThrough :: trainPerfOutput :: scorePerfOutput :: Nil
    val allTempFiles = trainModelOutput :: Nil

    allPipes foreach(_.file.deleteOnExit)
    allTempFiles foreach(_.deleteOnExit)

    val runner = Future {

      // TODO propagate failure back nicely.
      // Note that this isn't trivial, as both the writer to these
      // as well as the reader (main thread) would need to be notified.
      // Consider closing pipes as a simple option (if this breaks the block on either end)

      val (scriptFile,cwd) = script match {
        case StringSource(s) =>
          val scriptFile = getTempLocalFile(localDir, "script")
          scriptFile.deleteOnExit()
          writeToLocal(scriptFile, s)
          (scriptFile,None)
        case CacheSource(f) => (f.file,None)
        case FileSource(f) =>
          //Working directory change only relevant here, as this is the only case
          //where a file isn't renamed by hadoop.
          (f,Some(f.getParentFile))
      }

      val configFile = args.config.map { case str =>
        val cf = getTempLocalFile(localDir, "config")
        cf.deleteOnExit()
        writeToLocal(cf, str)
        cf
      }

      exec(scriptFile,
        List(args.layer.toString, trainInput.file.getAbsolutePath, args.numNumeric.toString, args.numFactor.toString,
          trainModelOutput.getAbsolutePath,"TRUE","TRUE","TRUE",
          trainOutput.file.getAbsolutePath, trainPerfOutput.file.getAbsolutePath) ++
          cachedLib.toList ++
          configFile.map(_.getAbsolutePath).toList ++
          args.extraArgs, cwd)

      uploadToHdfs(hadoopCmd)(trainModelOutput.getAbsolutePath, args.hdfsModelOutput)

      exec(scriptFile,
        List(args.layer.toString, scoreInput.file.getAbsolutePath, args.numNumeric.toString, args.numFactor.toString,
          trainModelOutput.getAbsolutePath,"FALSE","TRUE","TRUE",
          scoreOutput.file.getAbsolutePath,scorePerfOutput.file.getAbsolutePath) ++
          cachedLib.toList ++
          configFile.map(_.getAbsolutePath).toList ++
          args.extraArgs, cwd)

      //println("* runner done")
    }

    val writer = Future {
      new SequentialFileWriter[Action](
        (Train,trainInput.file) :: (Score,scoreInput.file) :: (TrainPerformance,trainPerfThrough.file) :: (ScorePerformance,scorePerfThrough.file) :: Nil iterator
      ).run(data)
    }

    def onCompletion = {
      //println("* cleanup")
      allPipes foreach(_.file.delete)
      allTempFiles foreach(_.delete)
      Unit
    }

    new SequentialFileIterator[Action](
      (Train,trainOutput.file) :: (TrainPerformance,trainPerfOutput.file) ::
      (Score,scoreOutput.file) :: (ScorePerformance,scorePerfOutput.file) ::
      (TrainPerformance,trainPerfThrough.file) :: (ScorePerformance,scorePerfThrough.file) :: Nil iterator, onCompletion _)
  }
}

object PipedModel {
  def apply(script : ScriptSource, localDir : String = System.getProperty("java.io.tmpdir"), hadoopCmd : String = "hadoop") = new PipedModel(script,localDir, hadoopCmd)
}


