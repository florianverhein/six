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
# Some modelling examples using basic preprocessing and modelling with rpart and glm2
# Can mix and match from here.
#
# Author: Florian Verhein


source('perf.R')

# Preprocessing
# -------------

# Build dictionary and statistics, including basic feature prunning.
buildDictionary <- function(data,fids=NA,max_levels=20,min_density=0.01,range_threshold=0) {
  
  if (is.na(fids))
    fids <- names(data)[3:ncol(data)]
  
  # Prune by density
  densities <- sapply(data[,fids],function(x) sum(!is.na(x))/length(x))
  ok <- which(densities >= min_density)
  ig <- length(fids)-length(ok)
  if (ig > 0) logger(sprintf("  Ignoring %d features because they have density < %f",ig,min_density))
  fids <- fids[ok]
  
  #Dictionary for Factors
  dict_f <- lapply(data[,fids],function(x){
    if(is.factor(x)) {
      # Don't use levels as some may not exist in this subset!
      return(sort(unique(as.character(x))))  
    } else {
      return(NULL)
    }
  })
  dict_f[sapply(dict_f, is.null)] <- NULL
  
  # Dictionary for Numerics
  dict_n <- lapply(data[,fids],function(x){
    if(is.numeric(x)) {
      return(c(range(x,na.rm=T),mean(x,na.rm=T),var(x,na.rm=T))) 
    } else {
      return(NULL)
    }
  })
  dict_n[sapply(dict_n, is.null)] <- NULL
  
  # Prune bad categoricals
  lvls <- lapply(dict_f, length)
  ok <- which(lvls <= max_levels & lvls > 1)
  ig <- length(dict_f)-length(ok)
  if (ig > 0) logger(sprintf("  Ignoring %d features because they don't have 1 > levels <= %d",ig,max_levels))
  dict_f <- dict_f[ok]
  
  # Prune bad numerics
  ranges <- lapply(dict_n, function(x) {x[2]-x[1]})
  ok <- which(ranges > range_threshold)
  ig <- length(dict_n)-length(ok)
  if (ig > 0) logger(sprintf("  Ignoring %d features because they don't have range > %d",ig, range_threshold))
  dict_n <- dict_n[ok]
  
  dictionary <- list(factors=dict_f,numerics=dict_n)      
  dictionary
}

# Data preprocessing via application of dictionary.
preProcess <- function(data,dictionary,scale="none"){
  
  # Ensure factors are consistent
  dict_f <- dictionary$factors
  data[,names(dict_f)] <- lapply(names(dict_f), function(fid,data,dict_f) {
    x <- data[,fid]
    ls <- dict_f[[fid]]
    factor(as.character(x), levels=ls)
  },data,dict_f)
  
  # Clamp and impute mean
  dict_n <- dictionary$numerics
  data[,names(dict_n)] <- lapply(names(dict_n), function(fid,data,dict_n) {
    x <- data[,fid]
    stats <- dict_n[[fid]]
    min <- stats[1]
    max <- stats[2]
    mean <- stats[3]
    var <- stats[4]
    x[which(x < min)] <- min
    x[which(x > max)] <- max
    x[which(is.na(x))] <- mean
    if (scale == "normalise"){
      x = (x-min)/(max-min)
    } else if (scale == "standardise") {
      x = (x-mean)/sqrt(var)      
    } else if (scale != "none") {
      stop(paste0("unknown scale: ",scale))
    }
    x
  },data,dict_n)
  
  data
}

# Training
# --------

# Null model
predict.NULL <- function(model,data){
  rep(NA,nrow(data))
}

trainTree <- function(data, fmla, ...) {

  library("rpart")
  logger("Building decision tree...")
  logger(fmla)
  fit.t <- rpart(fmla,data=data,x=FALSE,y=FALSE,model=FALSE,
                 control=rpart.control(cp=0.005),...)
  fit.t$where <- NULL #Save space
  logger(fit.t)
  logger(fit.t$variable.importance)
  fit.t
}

trainGlm <- function(data, fmla, dictionary) {

    library("glm2")
    logger("Building GLM...")
    fit.lm <- NULL
    n_n <- names(dictionary$numerics)
    if (length(n_n) > 0) {
      #Numerics only for now, till factor issue solved
      #fmla <- formula(paste0("label~",paste(n_n,collapse="+")))
      logger(fmla)

      family <- NULL
      if (is.numeric(data$label)) {
        family <- gaussian  #assumption for now
      }
      if (is.factor(data$label) & length(levels(data$label)) == 2) {
        family <- binomial(link="logit")
      }

      if (!is.null(family)) {
        fit.lm <- try(
          glm2(formula=fmla,family=family,data=data,x=FALSE,y=FALSE,model=FALSE)
        )
        if (inherits(fit.lm, "try-error")) {
          logger("Error while training GLM")
          fit.lm <- NULL
        } else {
          if (fit.lm$converged){

            # Save space
            fit.lm$data <- NULL
            fit.lm$linear.predictors <- NULL
            fit.lm$weights <- NULL
            fit.lm$prior.weights <- NULL
            fit.lm$residuals <- NULL
            fit.lm$fitted.values <- NULL
            fit.lm$effects <- NULL

            logger(fit.lm)

          } else {
            logger("GLM did not converge")
            fit.lm <- NULL
          }
        }
      } else {
        warning("Could not determing appropriate family for glm")
      }
    }
    fit.lm
}

trainTreeAndGlm <- function(data,scale,tree=F,glm=F,...){
  
  logger("Building dictionary...")
  dictionary <- buildDictionary(data)

  models <- list()
  fids <- c(names(dictionary$numerics),names(dictionary$factors))

  if (length(fids) == 0){
    w <- "No features left after building dictionary. Unable to build models. Will use Null models."
    logger(w)
    warning(w)
    if (tree) models <- append(models,list(NULL))
    if (glm) models <- append(models,list(NULL))
    return (list(dictionary=dictionary,models=models))
  }
  
  logger("Preprocessing data for training...")
  data <- preProcess(data,dictionary,scale)
  
  fmla <- formula(paste0("label~",paste(fids,collapse="+")))
    
  if (tree) {
    f <- trainTree(data,fmla,...)
    models <- append(models,list(f))
  }
  
  if (glm) {
    f <- trainGlm(data,fmla, dictionary) # TODO dictionary passed in for now, but intend to remove
    models<- append(models,list(f))
  }
  
  list(dictionary=dictionary,models=models)
}

# Scoring
# -------

# Scores arbitrary models after applying prrprocessing
scoreModels <- function(data,model) {

  logger("Preprocessing for scoring...")
  data <- preProcess(data,model$dictionary)
  
  scores <- if (length(model$models) == 0) {
    stop("Cannot score: model$models is empty")
    #data.frame()    
  } else {
    foreach (i = 1:length(model$models), .combine=cbind) %do% {
      logger(sprintf("Scoring model %d...",i))
      p <- predict(model$models[[i]],data)
      data.frame(p)
    }
  }
  scores
}

evaluatePerformance <- function(labels,scores){
  generalised_gini(scores,labels)
}




