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

#
# Main entry point for running a layer with rpart and glm
#
# Author: Florian Verhein

# Useful debugging when running on cluster
#print(sprintf("Starting in %s which contains:",getwd()))
#print(system('ls'))

source('piped_model_args_lib.R')
source('piped_model_run_lib.R')
source('piped_model_modelling_lib.R')
source('util.R')
source('logging.R')


args <- commandArgs(trailingOnly = TRUE)

goTreeAndGlm <- function(args, labelType = "numeric", scale="none", glmInLayer1 = FALSE) {

  params <- parseAndValidateArgs(args)

  if (!is.na(params$libs)) initialiseLibPaths(params$libs)

  library("foreach") # load libraries after lib path modification

  tree <- is.installed("rpart")
  glm <- is.installed("glm2")

  params <- adjustParamsForLayerAndLabelType(params,labelType)
  #logger(params)

  # Trying no glm in first layer. Works well (no degredation) once have decent sized data.
  if (!glmInLayer1 && params$layer == 1) {
    glm <- F
  }

  trainFun <- function(data) trainTreeAndGlm(data,scale,tree,glm)

  runGeneric(params, labelType, trainFun, scoreModels, evaluatePerformance)
}

withErrorHandling(function() goTreeAndGlm(args))
