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

# Example deployment script. Copies assembly and scripts.

VERSION="0-1-1"
JAR_VERSION="0.1.1"
DEPLOY_HOST="dn1"
DEPLOY_DIR="deploy/six_${VERSION}"

TO="$DEPLOY_HOST:$DEPLOY_DIR"
JAR="six-assembly-${JAR_VERSION}.jar"

ssh $DEPLOY_HOST mkdir $DEPLOY_DIR

scp target/${JAR} $TO
scp src/main/scripts/*.sh $TO
scp src/main/R/*.R $TO
