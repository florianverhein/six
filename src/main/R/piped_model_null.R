#!/usr/bin/env Rscript

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

# A sample layer implementation that effectively does nothing but excercises the 'framework'
#
# Author: Florian Verhein


source('piped_model_args_lib.R')
source('piped_model_run_lib.R')
source('util.R')
source('logging.R')

nullTrain <- function(data) {
  model <- c("Some model and dictionary and transformations")
  model
}

nullScore <- function(data,model){
  scores <- data$label + data[,3]*2
  scores <- ifelse(is.na(scores),0,scores)
}

nullPerf <- function(labels,scores){
  sqrt(sum((labels-scores)^2))
  #generalised_gini(scores,labels)
}

goNull <- function(args) {
  params <- parseAndValidateArgs(args)
  labelType <- "numeric"
  params <- adjustParamsForLayerAndLabelType(params,labelType)
  runGeneric(params,labelType,nullTrain,nullScore,nullPerf)
}

args <- commandArgs(trailingOnly = TRUE)

library("foreach")

withErrorHandling(function() goNull(args))

