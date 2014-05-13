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
# Simple performance Functions
#

# Measure of ranking. Between 0.5 and 1.
# Note that the optimal ranking for regression problems is usualy < 1.
# For classification problems, this measure is equivalen to the GINI: GINI = (gain_auc - 0.5) * 2
gain_auc <- function(p,y,na.rm=F) {
  if (na.rm) {
    subset <- intersect(which(!is.na(p)),which(!is.na(y))) # TODO need to de na Y for some reason. DEBUG!
    y <- y[subset]
    p <- p[subset]
  }
  y <- y - min(y) # So that the rest works if labels are < 0. Remeber, it's all about ranking.
  sums <- cumsum(y[order(-p)])
  sums <- sums / sums[length(sums)]
  auc <- sum(sums) / length(p)
  max(auc,1-auc) # In case of backwards ordering, p negatively associated. Useful if p is a feature.
}

# Computes a score between 0 and 1, where 0 means random ranking
# and 1 means the optimal ranking was achieved by p.
# Reduces to GINI when y is binary.
generalised_gini <- function(p,y,na.rm=F) {
  (gain_auc(p,y,na.rm) - 0.5) / (gain_auc(y,y,na.rm) - 0.5)
}
