
# Six

Six is a scalding tool/framework initially intended for easily building distributed stacked ensemble learners on Hadoop Map Reduce,
where the individual models making up the ensemble are built by user-supplied executable, such as scripts written in python, R, or your favourite language.

Six supports hetrogenous ensembles with up to two layers and as many different models as you like.
However, simple "special cases" are also useful: for example, you can use it to run a distributed grid search over parameters while learning a simple model.

Six is named after "number six", the only Cylon in the Battlestar Galactica television series who's distinct incarnations have their own names, due to an analogy between cylon model replication, learning and reincarnation and the way the "six" tool is implemented. We need nerdy names after all...

## Introduction

Six is designed for rapid prototyping of complex models at scale while using familiar scripting languages.
Six takes care of all the data replication, folding, feature bucketing, sampling, and data flow connections for you in an efficient way. It builds each layer in the ensemble with a single Map Reduce job.
All you need to provide is a script which can:

* Read a dataset
* Train a model on that dataset (including whatever pre-processing you want)
* Write the model to a file
* Score a dataset and write the scores to a file
* Optionally, evaluate the performance of the model and write that performance to a file

So basically... all you need is a script that you may already have used to build small scale models. Alternatively you can use one of the preexisting scripts in this project.
These scripts do not need to be scalable.

The framework supports ensembles consisting of two layers, and each layer can use a different script if you like. For example:

* You could use the same modelling script for both layers if you like (homogeneous stacked ensemble).
* Or you could use a model in the first layer, and a voting scheme in the second (standard/'non-stacked'/bagged ensemble).
* An approach that works well is to use decision trees in the first layer, and a linear model in the second layer. R scripts to do this are included.
This results in a stacked ensemble with homogenous layers. The decision trees also function as an effective feature selection method in this configuration.
* You can even build multiple models in the script (e.g. a tree and a linear model). In this case the layer is hetrogeneous. There is no limit to the number of models in a layer.
* Or you can just do feature selection in the first layer (in this case, the 'scores' are simply the values of the features that you have selected), and do the modelling in the second layer.

## How it Works (High Level)

Given such script(s), the framework will run it on many reducers with different subsets of instances (e.g. for cross validation) and different
buckets of features (e.g. each model in the first layer gets a different random subset of features)
and different configurations (e.g. grid search over modelling parameters).

Please note that the support for different configurations is work-in-progress and may change.

Suppose your layer 1 script builds M1 models and you have selected to use B buckets and F folds and C configurations. Suppose your layer 2 script builds M2 models.
For each fold and configuration, the output of layer 1 is B*M1 features (the scores output by the M1 models in each of the B buckets).
These are fed into the layer 2 script (there is only one bucket in layer 2). Here, for each fold and configuration, the layer 2 model(s) are built on the B*M1 features.
This produces M2 models (ensembles) for each configuration and fold.
Therefore, there are C*F*B invocations of the layer 1 script and C*F invocations of the layer 2 script.

At the end, for each fold and configuration, you get:

* Scores and performance for all layer 1 models on both training and test sets (for that fold)
* Scores and performance for all layer 2 models (the final ensemble) on both training and test sets (for that fold)

## Requirements

Requirements include:

* If using the included R scripts, R must be installed on the cluster
* For one of the job tests to pass, R must be installed on the machine you run them on
* Hadoop client configs must be installed on the cluster if you want the models to be available on hdfs  

## Example

An almost complete example (just lacks data) is provided in the deploy.sh and launchSixExample.sh scripts.
First, check DEPLOY_HOST and DEPLOY_DIR are ok in both scripts and fill in the required data variables in launchSixExample.sh.
You may also need to check that HADOOP_CMD is correct for your cluster installation. If you don't know what command to use
on your cluster (and "hadoop" doesn't work) or client configs are not installed on the nodes, just set it to "echo". This means model files will not be written to hdfs.

Then, build and deploy the project as follows:

    ./sbt assembly
    ./deploy.sh

SSH to $DEPLOY_TO and then:

    ./launchSixExample.sh

## Sampling for Rapid Prototyping

Hash sampling at both the instance and feature dimensions are supported. This enables you to build models on a small sample and iterate quickly, then scale up to big data easily. This is also one of the motivations for choosing an implementation that ensures each modelling layer runs in a single map reduce layer.

## Data Input and Preparation

Labels and feature data is read in either 3 or 4 tuple format coordinate.

The ensemble assumes a Dictionary exists (a scalding application exists to build this for you, including guessing feature types).

Other than correcting missing values using a user configured set (if a record's value matches any value in this set it will be dropped)
or dropping bad records (e.g. containing binary characters),  the framework does not alter the data. Any feature pre-processing should be done in the modelling scripts.

Internally, the data is represented as strings and is passed as is to the modelling scripts.

## Modelling Script details

See requirements in PipedModel.scala and examples in src/main/R/.

### Library Dependencies and Bootstrapping

If a library you need is not installed on the cluster (often the case whe prototyping), you can put it on hdfs and tell Six to make it available to the scripts via the distributed cache.

This results in a peculiar coding style in modelling scripts where, for example in R, the library() command is called within a function that is called after initialisation, rather than at the top of the script.

## Submitting Scripts

There are three ways to submit the scripts

1. Single script on the local filesystem (cannot reference any other scripts). The script is read on the submitter, closed over, and written to a temporary location on the reducer.
2. Single script on hdfs (cannot reference any other script). The script is placed on the distributed cache.
3. A directory of scripts, with the main script named. The main script can refer to other scrips within that directory (or deeper). The directory is placed on the distributed cache.

## Implementation Notes and Tricks

* Independent hash functions are used for key operations (fold assignment, sampling, bucketing, etc)
* Partial data replication is used (common strategy for parallel execution on map reduce). For example, the same fold of data will be sent to multiple reducers, sometimes for use as a test set and other times as a training set.
* IPC between Six and the scripts is done using multiple named pipes and the readers-writers paradigm.
* Data needs to be densified from a sparse coordinate input format before it can be used by the scripts. This is achieved in a streaming manner that relies on a specific sorting.
* Performance data is "passed-through" between layers, as it is not possible to output data from an internal layer of a map reduce job.
* Future improvement: find a way to turn of HashPartitioning.
The optimal number of reducers required so that one instance of a modelling script runs on each reducer is known by six.
Furthermore, the hashing load-balances data so that each reducer gets roughly equal ammounts of data.
However, since the MR framework uses hash-partitioning, some reducers will end up with multiple reduce tasks while others get none (hash on the bucket id).
So for example, if a reducer gets 2 reduce tasks, another will get none, and the layer will take roughly twice as long as it needs to.

## Known Bugs and Limitations

Limitations:

* This project started as a data science proof-of-concept exploration and has evolved from there. It is still maturing.
* Automated test coverage is, unfortunately, low.
* Ensuring a clean exit without hanging threads when a script fails is not easy. At present, the jvm on the reducer is exited. This achieves the required outcome (job failure rather than a hanging reducer awaiting data it will never get), but this could certainly be improved.
* Passing a particular configuration line to a script is not clean yet, and requires some improvement (this is a feature in progress). You can safely ignore this feature for now.
* Command line arguments for the scripts are simplistic at present. This is because libraries are typically used to read flags and key value arguments, and those libraries cannot be easily bootstrapped if they do not exist on the cluster.
Hence, the format was chosen to be easily processed without libraries. In future, there should be alternative formats.
* The BucketDictionary is not able to be written to hdfs at present (this needs to be done at the submitter). Instead it is written to stdout.

## TODO

* Some background about why ensembles are good (motivation) and what they are
* Sample data for self contained run?

