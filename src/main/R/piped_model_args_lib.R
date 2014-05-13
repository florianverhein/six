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

#
# Argument handling for interacting with PipedModel
#
# Author: Florian Verhein

usage <- function(){
  print("Usage: layer dataFile numNumeric numFactor modelFile train score perf [scoreFile] [perfFile] [libPath]")
  print("  - dataFile is psv organised as: iid | label | psv of values, where numNumeric values are followed by numFactor values")
  print("  - train, score, perf are R logicals and determine what shall be run")
  print("  - if score or perf is T, then the respective file argument is required (to which the scores and performance will be written).")
  quit("no",1)
}

parseAndValidateArgs <- function(args, usageFun = usage) {
  if (length(args) < 8)
    return(usageFun())
  layer <- as.numeric(args[1])
  dataIn <- args[2]
  numNumeric <- as.numeric(args[3])
  numFactor <- as.numeric(args[4])
  modelIO <- args[5]
  train <- as.logical(args[6])
  score <- as.logical(args[7])
  perf <- as.logical(args[8])
  argsUsed <- 8 + score + perf
  if (is.na(train) || is.na(score) || is.na(perf) || length(args) < argsUsed || length(args) > argsUsed+1)
    return(usageFun())
  scoresOut <- ifelse(score,args[9],NA)
  perfOut <- ifelse(perf, args[9 + score],NA)
  libs <- NA
  if (length(args) == argsUsed +1)
    libs <- args[argsUsed +1]
  list(layer=layer,dataIn=dataIn,numNumeric=numNumeric,numFactor=numFactor,
       modelIO=modelIO,train=train,score=score,perf=perf,
       scoresOut=scoresOut,perfOut=perfOut,argsUsed=argsUsed,
      libs=libs)
}

# If this is the 2nd (or greater) layer, assume that all models in the first layer
# output the labelType. Can then disambiguate numFactor and numNumeric.
adjustParamsForLayerAndLabelType <- function(params, labelType) {
  if (params$layer > 1) {
    if (labelType == "numeric") {
      params$numFactor = 0
    } else if (labelType == "factor") {
      params$numNumeric = 0
    } else {
      logger("Unknown labelType")
      quit("no",1)
    }
  }
  params
}

