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

# Simple generic runner
#
# Author: Florian Verhein

# TODO Consider optimising for case where scoring data = training data... duplicate pre-processing not ideal
# TODO handle case if train or score is empty

# This is used for default IO
source('piped_model_io_lib.R')

# Runs a modelling pipeline as defined by supplied functions and the params.
runGeneric <- function(params, labelType, trainFun, scoreFun, perfFun, readDataFun = readData, writeDataFun = writeData) {

  logger("Reading Data...")
  data <- readDataFun(params$dataIn, labelType, params$numNumeric, params$numFactor)
  model <- NULL
  scores <- NULL

  if (params$train) {
    logger("Training...")
    model <- trainFun(data)
    save(model,file = params$modelIO)
    #throw error if exists?
  } else {
    load(params$modelIO)
  }

  if (params$score || params$perf) {
    logger(sprintf("Scoring..."))
    scores <- scoreFun(data,model)
  }

  if (params$score) {
    output <- cbind(data[,c("iid","label")],scores)
    writeDataFun(output,params$scoresOut)
  }

  if (params$perf) {
    logger("Evaluating performance...")
    perfs <- foreach (i = 1:length(scores), .combine=cbind) %do% {
      p <- perfFun(data$label,scores[[i]])
      data.frame(p)
    }
    output <- cbind(data.frame(iid=params$layer,label=NA),perfs)
    writeDataFun(output,params$perfOut)
  }
}
