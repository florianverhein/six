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


# Useful functions for error handling and adding R libraries on the fly.
#
# Author: Florian Verhein

# R library handling
# ------------------

is.installed <- function(mypkg) is.element(mypkg, installed.packages()[,1])

initialiseLibPaths <- function(lib_path) {
  if (file.exists(lib_path)) {
    .libPaths(c(.libPaths(),lib_path))
    logger("R library paths now in use:")
    logger(.libPaths())
  } else {
    logger(sprintf("Library path does not exist: %s",lib_path))
  }
}

# Error Handling
# --------------

# Wrap a function with useful implementation of try catch
withErrorHandling <- function(FUN) {

    result = tryCatch({
        FUN()
    }, error = function(e) {
        logger("*** Error ***")
        logger(e)
        #logger(geterrmessage())
        #stop()
        quit("no",10,T)
    }, finally = {
        w <- warnings()
        if (length(w)>0) {
            logger("*** Warnings ***")
            logger(w)
        }
    })
}
