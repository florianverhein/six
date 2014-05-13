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

# Basic logging functions
# Hook for the future.

LOG_ERROR = 1
LOG_INFO = 2
LOG_DEBUG = 3

LOG_DEFAULT = LOG_DEBUG
LOG_LEVEL = LOG_DEBUG


logger <- function(msg,level=LOG_DEFAULT) {
  if (level <= LOG_LEVEL)
     print(msg)
}
