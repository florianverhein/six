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

import scala.reflect.ClassTag

import org.apache.hadoop.conf.Configuration

import com.twitter.scalding._
import com.twitter.scalding.TDsl._
import com.twitter.scalding.typed.Grouped
import com.twitter.scalding.typed.ValuePipe
import com.twitter.scalding.WritableSequenceFile
import com.twitter.scalding.filecache.{CachedFile, DistributedCacheFile}

import com.cba.omnia.edge.source.template.TemplateCsv
import java.io.File
import au.com.cba.omnia.six.piped._
import scala.Some
import com.twitter.scalding.Hdfs
import au.com.cba.omnia.six.util.FlateningIterator
import scala.Some
import com.twitter.scalding.Hdfs
import com.cba.omnia.edge.source.template.TemplateCsv

/**
 * All required preparation of data for the different layers
 *
 * @author Florian Verhein
 */
trait LayerPreparation extends RecordTransformations {

  type B = Int; type Fold = Int; type FI = Int; type C = Option[Int]

  /**
   * Applies the bucket replication and feature renaming as defined by the bucket dictionary
   **/
  def applyBucketing[I,L,F,V](dict : BucketDictionary[B,F])(r : SparseRecord[I,L,F,V]) : Iterable[SparseRecord[I,L,FI,V]]  = {
    dict.byFeature.get(r.fid) match {
      case Some(buckets) => buckets.map{
        case bucket =>
          val bDict = dict.byBucket(bucket)
          val nfid = bDict.dictionary(r.fid).fBucketIndex
          val featsInBucket = bDict.dictionary.size
          //SparseRecord[I,L,FI,V](bucket,r.fold,r.action,r.iid,r.label,nfid,r.value,Some(featsInBucket))
          r.copy(bucket = bucket, fid = nfid, valuesLength = Some(featsInBucket))
      }
      case default => Nil
    }
  }

  /**
   * Applies the folding function to replicate records according to their assigned fold.
   */
  def applyFolding[I,L,F,V](foldingFunction : Int => Iterable[(Fold,Action)])(r : SparseRecord[I,L,F,V]) : Iterable[SparseRecord[I, L, F, V]] =
    foldingFunction(r.fold).map {
      case (fold, action) =>
        //SparseRecord[I,L,F,V](r.bucket,fold,action,r.iid,r.label,r.fid,r.value,r.valuesLength)
        r.copy(fold = fold, action = action)
    }

  /**
   * Applies replication required to evaluate the configs
   */
  def applyConfigs[I,L,F,V](configs : Option[Iterable[Int]])(r : SparseRecord[I,L,F,V]) : Iterable[SparseRecord[I, L, F, V]] = {
    configs match {
      case None => List(r)
      case Some(cs) => cs.map( c => r.copy(cid = Some(c)))
    }
  }

  def prepareInitialLayerSparse[I,L,V](pipe : TypedPipe[SparseRecord[I,L,FI,V]], buckets : Int, folds : Int, configs : Option[Int])(
    implicit iord : Ordering[I]) : Grouped[(C,B,Fold),SparseRecord[I,L,FI,V]] = {
    val reducers = buckets * folds * configs.getOrElse(1)
    // TODO figure out how can turn hash-partitioning off, as this screws up the perfect partitioning by bucket that I do here,
    // resulting in reducers with multiple groups and some with none.
    pipe.groupBy( r => (r.cid, r.bucket, r.fold)) //TODO add config
      .sortBy(r => (r.action,r.iid)) //Secondary sort required for flattening
      .withReducers(reducers)
  }

  def prepareInitialLayer[I,L,V : ClassTag](pipe : TypedPipe[SparseRecord[I,L,FI,V]], missing : V, buckets : Int, folds : Int, configs : Option[Int])(
    implicit iord : Ordering[I]) : Grouped[(C,B,Fold),DenseFlatRecord[I,L,V]] = {
    prepareInitialLayerSparse(pipe, buckets, folds, configs).mapValueStream(flattenToDense[I,L,V](missing))
  }

  /**
   * Note: Take care that stream is sorted apropriately. E.g. By fold, action, iid.
   * Given dense flat records accross multiple buckets, combine them together into one bucket with bucketId
   * so that each resulting dense flat record contains values from original bucket 0, then bucket 1, etc...
   * Note that it is possible for a bucket to be missing for an instance if no features in that instance are assigned to the bucket.
   */
  def flattenBuckets[I,L,V : ClassTag](buckets : Int, newBucketId : Int, missing : V)(stream : Iterator[DenseFlatRecord[I,L,V]]) : Iterator[DenseFlatRecord[I,L,V]] = {

    def flattenAndDensify(values : List[(Int,Array[V])]) : Array[V] = {
      val nestedSize = values.head._2.length //same for all
      val rowLength = buckets*nestedSize
      values.foldLeft(Array.fill(rowLength)(missing)){
        case (array,(fid,vals)) =>
          for (i <- 0 until nestedSize) {
            array(fid*nestedSize + i) = vals(i)
          }
          array
      }
    }

    FlateningIterator(
      stream.map(r => ((r.cid,r.fold, r.action, r.label, r.iid), r.bucket, r.values))
    ).map{
      case ((cid,f,a,l,iid),vals) =>
        val nvals = flattenAndDensify(vals)
        //val nvals = vals.sortBy(_._1).flatMap(_._2).toArray
        //Note: ^ This breaks when an instance has no features in a bucket
        DenseFlatRecord(cid,newBucketId,f,a,iid,l,nvals)
    }
  }

  /**
   * Suppose each bucket i produces output from n models (i.e. n new features). These are all combined into a single
   * bucket 0 with buckets*n features.
   * This supports a single stacked six of hetrogenous layer 1 models.
   */
  def prepareFinalLayer[I,L,V : ClassTag](pipe : TypedPipe[DenseFlatRecord[I,L,V]], missing : V, buckets : Int, folds : Int, configs : Option[Int])(implicit iord : Ordering[I]) : Grouped[(C,B,Fold),DenseFlatRecord[I,L,V]] = {
    val bucketId = 0
    val reducers = folds * configs.getOrElse(1)
    pipe.groupBy(r => (r.cid,bucketId,r.fold))
    .sortBy(r => (r.action,r.iid,r.bucket))
    .withReducers(reducers)
    .mapValueStream(flattenBuckets(buckets,bucketId,missing) _)
  }

  /**
   * Suppose each bucket i produces output from n models (i.e. n new features).
   * These are separated into n buckets with buckets features
   * This supports parallel evaluation of n independent stacked ensembles each with a homogeneous layer 1.
   */
  def prepareFinalLayerParallel[I,L,V](pipe : TypedPipe[DenseFlatRecord[I,L,V]]) : Grouped[(B,Fold),DenseFlatRecord[I,L,V]] = {
    //TODO: de-flatten, then group by features (new bucket) and fold.
    //sort by action, iid, original bucket
    ???
  }

}

/**
 * "Six" scalding application for building stacked ensembles.
 *
 * TODO refactor and clean up
 *
 * @author Florian Verhein
 */
class Six(args: Args) extends Job(args)
    with JobCommon
    with InputPreparation[String]
    with BucketDictionaryTools[Int,String]
    with LayerPreparation
{

  type InRF = InputRecord[I,F,V]
  type InRL = InputRecord[I,F,L]

  val runMode = args.getOrElse("mode","model2")
  val output = args("output")
  val folds = args.getOrElse("folds","3").toInt
  val iidS = args.getOrElse("sample-instances","1").toInt
  val fidS = args.getOrElse("sample-features","1").toInt
  val buckets = args("buckets").toInt
  val bucketRep = args.getOrElse("bucket-replication","1").toInt
  val missingValues = getMissingValues(args)
  val missing = args.getOrElse("missing-internal","NA")

  // Functions that change key behaviours. TODO allow configuration from args
  val sampleIidF = hashSample(iidS) _
  val sampleFidF = hashSample(fidS) _
  val assignFoldF = assignFold(folds) _
  val assignBucketsF = BucketingFunctions.distributeDeterministicRandomly(buckets,bucketRep) _
  val foldingFunction = FoldingFunctions.crossValidation(folds) _

  val cachedLib: Option[CachedFile] = args.optional("hdfs-lib").map(path => DistributedCacheFile(path))

  import ValueType._

  // ==== Bucket Dictionary Preparation ====

  //TODO refactor this out
  val dictionary : Map[F,(FI,ValueType)] = TypedPsv[(F,Int,String,Int,Int)](args("dictionary"))
    .readAtSubmitter[(F,Int,String,Int,Int)]
    .filter( x => sampleFidF(x._1))
    .map(x => (x._1,(x._2,stringToValueType(x._3)))).toMap

  val bucketDictionary = makeBucketDictionary(dictionary,assignBucketsF)

  val hdfsConfig : Option[Configuration] = mode match {
    case Hdfs(_, config) => None
    //case Hdfs(_, config) => Some(config)
    //TODO the above breaks... job config is not serialisable and for some reason, attempt is made to serialise it :(
    //java.lang.RuntimeException: Neither Java nor Kyro works for class: class com.twitter.scalding.typed.FilteredFn instance: <function1>
    case default => None
  }

  val bucketDictionaryStr = bucketDictionary.byBucket.mkString("\n")

  hdfsConfig match {
    case Some(conf) => com.cba.omnia.edge.hdfs.Hdfs.write(
      new org.apache.hadoop.fs.Path(args("output-dict")),bucketDictionaryStr).run(conf)
    case _ =>
      println("=================")
      println(bucketDictionaryStr)
      println("=================")
  }

  // ==== Input Preparation ====

  val features = getFeaturePipe(args)
  val labels = getLabelPipe(args)
  val configs = getConfigs

  //println(configs)

  val l : Grouped[I,(InRL,Fold)] = labels.map(InputRecord.fromTuple[I,F,L])
    .filter(r => sampleIidF(r.iid))
    .map(r => (r, assignFoldF(r.iid)))
    .groupBy(_._1.iid)

  //TODO [new feature] generalise folding: Allow assignment to multiple folds here and interpret/use fold concept differently
  //to enable resampling, bootstrapping, etc. However, this means the assignment to train, test etc will also need to be done here.
  //so can no longer split these concepts up.
  //so: if doing it here, will need to change fold to Iterable[(Fold,Action)]

  val f : Grouped[I,InRF] = features.map(InputRecord.fromTuple[I,F,V])
    .filter(r => sampleIidF(r.iid) && sampleFidF(r.fid) &&
                 !isBad(r.fid) && !isBad(r.value) &&
                 !missingValues.contains(r.value))
    .groupBy(_.iid)

  val fl : TypedPipe[SparseRecord[I,L,F,V]] = f.hashJoin(l).map {
    case (_,(fr,(lr,fold))) => SparseRecord[I,L,F,V](None,-1,fold,Unassigned,fr.iid,lr.value,fr.fid,fr.value)
  }

  val flreplicated : TypedPipe[SparseRecord[I,L,FI,V]] =
    fl.flatMap(applyBucketing(bucketDictionary))
      .flatMap(applyFolding(foldingFunction))
      .flatMap(applyConfigs(configs.map(_.map(_._1))))

  // ==== Run a layer ====

  def extractShardSpecificInfoAndRunWith(
    encode : (I,L,Array[V]) => String,
    decode : String => (I,L,Array[V]),
    func : (Iterator[(Action,String)], PipedModelArgs) => Iterator[(Action,String)],
    layer : Int,
    shardPrefix : Option[String] = None,
    configs : Option[Map[Int,String]] = None,
    extraArgs : List[String] = Nil)
    (data : Iterator[DenseFlatRecord[I,L,V]])  :
      Iterator[DenseFlatRecord[I,L,V]] = {

    val it = data.buffered
    val head = it.head
    val bucket = head.bucket
    val fold = head.fold
    val numVals = head.values.size
    val config = head.cid

    def shardIdStr(layer: Int, bucket: Int, fold : Int, sep : String = "_") =
      "L" + layer.toString + sep + "B" + bucket.toString + sep + "F" + fold.toString + "C"+config.toString

    val shardId = shardIdStr(layer,bucket,fold)
    val modelOut = shardPrefix.getOrElse("") + shardId //TODO turn this into a file...

    val bDict = bucketDictionary.byBucket(bucket)
    val bInfo = bDict.info

    def foundError(r: DenseFlatRecord[I, L, V], extra: String = "") =
      sys.error("Unexpected number of values in shard " + shardId + ": numVals = " + numVals + " but " + bInfo + " and record = " + r + extra)

    val (numNumeric, numFactor) = if (layer == 1) {
      if (bInfo.numNumeric + bInfo.numFactor != numVals)
        foundError(head)
      (bInfo.numNumeric, bInfo.numFactor)
    } else {
      //By convention, use numFactor = numNumeric = numVale in this case. Note: since we have no way of knowing what the first layer did,
      //we don't know which the types of the features produced by that layer. Hence, this makes the most sense.
      //The modelling script however knows what it did, so can act appropriately using this information.
      (numVals, numVals)
    }

    val configStr = config.flatMap(c => configs.map(m => m(c)))

    // This logging is quite useful and appears in the task tracker logs.
    println("==================== Running shard " + shardId + " ========================")
    val first = (head.action, encode(head.iid, head.label, head.values))
    println("First record      = " + first)
    if (layer == 1) {
      //TODO Note that ATM we don't have a bucket dict for other layers. Feature for later.
      val feats = bDict.info.features.mkString("|")
      println("Features in block = " + feats)
    }
    println("========================================================================")


    val modelArgs = PipedModelArgs(layer, numNumeric, numFactor, modelOut, cachedLib, configStr, extraArgs)

    func(it.map {
      case r =>
        if (r.values.length != numVals)
          foundError(r)
        (r.action, encode(r.iid, r.label, r.values))
    }, modelArgs).map {
      case (action, str) =>
        val (iid, label, values) = decode(str)
        val r = DenseFlatRecord(head.cid,head.bucket, head.fold, action, iid, label, values)
        if (action == Train || action == Score) {
          //TODO may be useful to assert that each row has same number of values
        } else if (action == TrainPerformance || action == ScorePerformance) {
          //TODO "
        }
        r
    }
  }

  def runLayer(layer : Int,
    func : (Iterator[(Action,String)], PipedModelArgs) => Iterator[(Action,String)],
    configs : Option[Map[Int,String]] = None,
    sep : Char = '|')
    (pipe : Grouped[(C,B,Fold),DenseFlatRecord[I,L,V]]) : Grouped[(C,B,Fold),DenseFlatRecord[I,L,V]] = {

    def encode(i : I, l : L, values : Array[V]) =  i + sep + l + sep + values.mkString(sep.toString)

    def decode(s : String)(implicit desI : String => I, desL : String => L, desV : String => V) = {
      val spl = s.split(sep)
      (desI(spl(0)),desL(spl(1)),spl.drop(2).map(desV).toArray)
    }

    val modelOutputPrefix = args("output-models-prefix") //Hdfs prefix (e.g. directory) to store models

    pipe.mapValueStream(extractShardSpecificInfoAndRunWith(encode,decode,func,layer,Some(modelOutputPrefix),configs) _)
  }

  // ==== Misc ====

  /**
   * Handle ways a set of configurations can be provided (local file, file on hdfs)
   */
  def getConfigs(): Option[Map[Int,String]] = {
    val arg = "configs"
    val harg = "hdfs-configs"
    val configs = if (args.boolean(arg) && !args.boolean(harg))
      Some(scala.io.Source.fromFile(new java.io.File(args(arg))).getLines.toList
        .map{ case x =>
          val s = x.split('|')
          (s(0).toInt,s(1))
        })
    else if (!args.boolean(arg) && args.boolean(harg))
      Some(TypedPsv[(Int,String)](args(harg)).readAtSubmitter[(Int,String)].toList)
    else if (args.boolean(arg) && args.boolean(harg))
      throw new Exception("Can't have both --"+arg+" and --"+harg+")")
    else None
    configs.map(_.toMap)
  }

  /**
   * Handle the different ways that a script can be provided (local file, file on hdfs, directory on hdfs)
   */
  def getScriptSource(layer : Int) = {
    val arg = "script-"+layer
    val carg = "hdfs-script-"+layer
    val cdarg = "hdfs-script-dir"
    val cfarg = "hdfs-script-name-"+layer

    val argOptions = List(args.boolean(cdarg) && args.boolean(cfarg), args.boolean(arg), args.boolean(carg))
    if (argOptions.map(x => if (x) 1 else 0).sum != 1)
      throw new Exception("Expecting one of --"+arg+" xor --"+carg + " xor (--"+cdarg+" and --"+cfarg+")")

    if (argOptions(0)) {
      val dir = args(cdarg)
      val cdir = DistributedCacheFile(dir)
      val scriptName = args(cfarg)
      val scriptFile = new File(cdir.file,scriptName)
      FileSource(scriptFile)
    }
    else if (argOptions(1)) {
      val scriptFile = args(arg)
      StringSource(scala.io.Source.fromFile(new java.io.File(scriptFile)).getLines.mkString("\n"))
    }
    else if (argOptions(2)) {
      val scriptFile = args(carg)
      CacheSource(DistributedCacheFile(scriptFile))
    }
    else sys.error("logic bug")
  }

  def getPipedModel(layer : Int) = {
    val script = getScriptSource(layer)
    PipedModel(script,nodeLocalDir,hadoopCmd).run _
  }

  // ==== Connection and Control ====

  val pipe = flreplicated

  val hadoopCmd = args.getOrElse("node-hadoop-cmd","hadoop")
  val nodeLocalDir = args.getOrElse("node-local-dir",System.getProperty("java.io.tmpdir"))

  val numConfigs = configs.map(_.size)

  runMode match {
    case "prep-raw" => writeTuples(prepareInitialLayerSparse(pipe,buckets,folds,numConfigs))
    case "prep-sparse" => writeSparse(prepareInitialLayerSparse(pipe,buckets,folds,numConfigs))
    case "prep-dense" =>  writeDense(prepareInitialLayer(pipe,missing,buckets,folds,numConfigs))

    case "model1" =>
      val model1 = getPipedModel(1)
      val layer1in = prepareInitialLayer(pipe,missing,buckets,folds,numConfigs)
      val layer1out = runLayer(1,model1,configs)(layer1in)
      writeDense(layer1out)

    case "model2" =>
      val model1 = getPipedModel(1)
      val model2 = getPipedModel(2)
      val layer1in = prepareInitialLayer(pipe,missing,buckets,folds,numConfigs)
      val layer1out = runLayer(1,model1,configs)(layer1in)
      val layer2in = prepareFinalLayer(layer1out.toTypedPipe.map(_._2),missing,buckets,folds,numConfigs)
      val layer2out = runLayer(2,model2,configs)(layer2in)

      if (args.boolean("dont-split-output")) writeDense(layer2out)
      else writeDenseSplit(layer2out) //primarily for testing purposes

    case default => throw new Exception("Unknown option value: " + runMode)
  }

  // ==== Output Helpers ====

  def writeTuples(pipe : Grouped[(C,B,Fold),SparseRecord[I,L,FI,V]]) =
    pipe.toTypedPipe.values.map(_.toTuple).write(TypedPsv[(Option[Int],B, Fold, Action, I, L, FI, V, Option[FI])](output))

  def writeSparse(pipe : Grouped[(C,B,Fold),SparseRecord[I,L,FI,V]]) =
    pipe.mapValueStream(flattenToSparse)
      .mapValues {
      case r =>
        val nvalues = r.values.map(x => x._1 + ":" + x._2)
        (r.action,r.iid,r.label,nvalues.mkString(","))
    }.toTypedPipe
      .write(TypedPsv[((C,B, Fold),(Action, I, L, String))](output))

  def writeDense(pipe : Grouped[(C,B,Fold),DenseFlatRecord[I,L,V]]) = {
    pipe
      .mapValues(r => (r.action,r.iid,r.label,r.values.mkString(",")))
      .toTypedPipe
      .write(TypedPsv[((C, B,Fold),(Action,I,L,String))](output))
  }

  def writeDenseSplit(pipe : Grouped[(C,B,Fold),DenseFlatRecord[I,L,V]]) = {
    pipe
      .mapValues(r => (r.action,r.iid,r.label,r.values.mkString("|")))
      .toTypedPipe
      .map {case ((cid,bucket,fold),(action,iid,label,values)) => (cid,bucket,fold,action,iid,label,values)}
      .toPipe('cid,'bucket, 'fold, 'action, 'iid, 'label, 'values)
      .write(TemplateCsv(
      output,
      "%s",
      'action,
      separator = "|",
      fields = ('cid,'bucket, 'fold, 'action, 'iid, 'label, 'values)
    ))
  }
}
