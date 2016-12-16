# Introduction #
Dense subgraph miner is an open source graph mining library written in Java and is released under the Apache License 2.0. The library uses the map-reduce methodology and its open source [Hadoop](http://hadoop.apache.org/) implementation, in order to discover dense subgraphs (DSD) within a given big data graph as well as various utilities like triangulation. This document contains only a short brief summary of the project structure as also some tutorials in how to build and use this software. For more up to date information about the project, changelog and issues, please check the links below.

* [Dense Subgraph Discovery in MapReduce](http://ikee.lib.auth.gr/record/131717/files/GRI-2013-10394.pdf?version=1)
* [Repository](https://github.com/tzeikob/dense-subgraph-miner)
* [Bug Reports](https://github.com/tzeikob/dense-subgraph-miner/issues)
* [Contributors](https://github.com/tzeikob/dense-subgraph-miner/graphs/contributors)

This library can be used in any project focusing on data mining and knowledge discovery on graphs and especially on discovery of possible dense parts of a given big data graph using triangulation. It provides a set of various features given a big data graph like,

* converting directed graphs to undirected,
* enumerating triangles,
* enumerating dense subgraphs

# Building from Source #
## Prerequisities ##

In order to build this project you need the following software pre installed in your system,

* Java JDK 8+
* Apache Maven 3+
* Git

## Build the Executable ##
Dense subgraph miner currently does not offering any option to download binaries, so in case you want to used it as an executable you have to clone and build it in your system.

```
git clone git@github.com:tzeikob/dense-subgraph-miner.git
cd dense-subgraph-miner/
mvn clean package
```

In the `target/` forlder you will find the `dense-subgraph-miner-<version>.jar` file as well as the `lib/` classpath folder containing all the external libraries the project depends on.

## Running a Hadoop Job ##
Having the executable file `dense-subgraph-miner-<version>.jar` and the `lib/` classpath folder you can now run map-reduce jobs. Before you post the first job you have to copy the libraries from the classpath folder `lib/` to the classpath folder of the Hadoop home directory `HADOOP_HOME/lib/` for each JVM of the complete cluster. Then you can post a map-reduce job by executing in the command line the following line,

```
hadoop jar dense-subgraph-miner-<version>.jar <job-entry> [genericOptions] <args>
```

where the `<job-entry>` is the name of the map-reduce job entry, the `[genericOptions]` are optional arguments like `-D mapred.child.java.opts=-Xmx1024m` and the `<args>` are the required arguments regarding the selected map-reduce job entry. Please read further to find detailed examples of supported map-reduce jobs.

### Converting a Directed Graph into an Undirected ###
Assuming you have put in the DFS a directed graph given as a list of edges with integer vertices per line like so,

```
1,3
3,2
...
5,4
```

you can convert it to an undirected graph just by posting the following command,

```
hadoop jar dense-subgraph-miner.jar EdgeUndirection <input> <delimiter> <rho> <tasks> <output>
```

where the required arguments are the `<input>` as the path in DFS to data of an undirected graph given as a list of edges per line, the `<delimiter>` as the character used in order to seperate the integer vertices of each edge, the `<rho>` as the number of disjoint edge partitions, the `<tasks>` as the number of the reducer tasks used and the `<output>` as the path in DFS to save the edge list of the new undirected graph.
