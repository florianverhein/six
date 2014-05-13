#!/bin/sh

#   Copyright 2014 Commonwealth Bank of Australia
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.

set -vx

# Example script demonstraing one way that Six may be run using the sample R modelling scripts to build a stacked ensemble.
#
# Fill in the data TODO's, then run as ./launchSixExample.sh

VERSION="0-1-1"
JAR_VERSION="0.1.1"
# NOTE: need to put full path in here for the model output to work (TODO -- find out why)
DEPLOY_DIR="/user/$USER/deploy/six_${VERSION}"

# ===== Data  ======

HDFS_FEATURES="TODO"
HDFS_LABELS="TODO"
DATA_TAG="TODO"

# ==== Output ====

HDFS_BASE_OUT="${DEPLOY_DIR}/${DATA_TAG}/${FEATURE_TYPE_NAME}"
hadoop fs -mkdir -p ${HDFS_BASE_OUT}
HDFS_OUT="${HDFS_BASE_OUT}/out"
HDFS_OUT_DICT="${HDFS_BASE_OUT}/dict"
HDFS_OUT_MODELS="${HDFS_BASE_OUT}/models/"  #NOTE: the trailing '/' here
hadoop fs -rm -r ${HDFS_OUT}
#hadoop fs -rm -r ${HDFS_OUT_DICT} # Do this manually. Allows dictionary re-use if same data.
hadoop fs -rm -r ${HDFS_OUT_MODELS}

# Common
JAR="six-assembly-${JAR_VERSION}.jar"
CARGS="--hdfs"
MISSING="-999,-9999,-99999"

# === Scripts ===

# Assumes you want all R scripts in the current dir.

HDFS_SCRIPTS="${HDFS_BASE_OUT}/scripts"
hadoop fs -mkdir -p $HDFS_SCRIPTS
hadoop fs -rm "${HDFS_SCRIPTS}/*"
hadoop fs -copyFromLocal *.R ${HDFS_SCRIPTS}
hadoop fs -ls ${HDFS_SCRIPTS}

# Dictionary
# ==========

APP="au.com.cba.omnia.six.CreateDictionary"
ARGS="--features $HDFS_FEATURES \
--missing-values $MISSING \
--output $HDFS_OUT_DICT"
if hadoop fs -test -e $HDFS_OUT_DICT
then 
  echo "WARNING: Using existing dictionary: $HDFS_OUT_DICT"
else
  echo "`date` Creating dictionary..."
  hadoop jar $JAR com.twitter.scalding.Tool $APP $CARGS $ARGS 
fi

# Six Ensemble
# ============

# Hadoop cmd that works on cluster nodes.
HADOOP_CMD="/opt/cloudera/parcels/CDH/bin/hadoop"
#HADOOP_CMD="hadoop"

# R libraries to be put in distributed cache for scripts to use
#CACHE="/datascience/rlibs"

HDFS_DICTIONARY=$HDFS_OUT_DICT

# Some sample sampling and bucketing configurations...
#B=101; BR=2; SI=1; SF=1
#B=71; BR=2; SI=1; SF=1
#B=43; BR=1; SI=1; SF=1
B=11; BR=1; SI=13; SF=7
#B=11; BR=1; SI=7; SF=7
#B=21; BR=2; SI=9; SF=3

SCRIPT_NAME_1="piped_model_tree_and_glm.R"
SCRIPT_NAME_2=$SCRIPT_NAME_1

APP="au.com.cba.omnia.six.Six"
ARGS=$@
ARGS="--features $HDFS_FEATURES \
--labels $HDFS_LABELS \
--missing-values $MISSING \
--dictionary $HDFS_DICTIONARY \
--output $HDFS_OUT \
--output-dict $HDFS_OUT_DICT \
--output-models-prefix $HDFS_OUT_MODELS \
--buckets $B \
--bucket-replication $BR \
--sample-instances $SI \
--sample-features $SF \
--node-hadoop-cmd $HADOOP_CMD \
--hdfs-script-dir $HDFS_SCRIPTS \
--hdfs-script-name-1 $SCRIPT_NAME_1 \
--hdfs-script-name-2 $SCRIPT_NAME_2 \
$ARGS"

#--hdfs-lib $CACHE \ #add in if needed

echo "`date` Launching Ensemble"
hadoop jar $JAR com.twitter.scalding.Tool $APP $CARGS $ARGS
echo "`date` Completed"

hadoop fs -cat "$HDFS_OUT_MODELS/*"

hadoop fs -cat "$HDFS_OUT/TrainPerformance/*"
hadoop fs -cat "$HDFS_OUT/ScorePerformance/*"

