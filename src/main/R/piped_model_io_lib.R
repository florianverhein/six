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
# Useful IO handling functions
#
# Author: Florian Verhein


# Detetmines how to read from src. Named pipes must be handled differently from normal files.
selectIO <- function(src) {
  if (!file.exists(src)) {
    file(src)
  } else {
    r <- system(sprintf("test -p %s", src))
    if (r == 0) {
      fifo(src)
    } else {
      file(src)
    }
    #...add more here if needed
  }
}


#TODO Consider changing to LaF
readData <- function(src, labelType = NA, numNumeric = NA, numFactor = NA) {
  actualSrc <- selectIO(src)
  iidType <- "factor"
  logger(paste0(sprintf("Reading labelType = %s, %d numerics and %d factors from %s = %s ...",labelType,numNumeric,numFactor,actualSrc,src)))
  colClasses <- NA
  col.names <- NULL
  knownTypes = F
  if (!is.na(numNumeric) && !is.na(numFactor) && !is.na(labelType)) {
    colClasses = c(iidType,labelType,rep("numeric",numNumeric),rep("factor",numFactor))
    col.names = c("iid","label")
    if (numNumeric > 0)
      col.names <- c(col.names,sprintf("N%d",seq(1:numNumeric)-1))
    if (numFactor > 0)
      col.names <- c(col.names,sprintf("F%d",numNumeric+seq(1:numFactor)-1))
    knownTypes <- T
  }
  #logger(colClasses)
  #logger(col.names)
  d <- read.table(actualSrc, header=F, sep="|", col.names = col.names, colClasses = colClasses)
  if (!knownTypes) {
    n <- names(d)
    names(d) <- c("iid","label",n[1 : (length(n)-2)])
  }
  logger(dim(d))
  logger(head(d))
  d
}

writeData <- function(data,dest) {
  logger(paste0(sprintf("Writing to %s ...",dest)))
  write.table(data,file=selectIO(dest),row.names=F,col.names=F,quote=F,sep="|")
  logger(dim(data))
  logger(head(data))
}
