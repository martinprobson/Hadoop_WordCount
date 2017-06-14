# MapReduce Word Count Example

## Summary

This a the Java MapReduce Word Count example.

## Python Version

An example of the same Map/Reduce logic using Python locally is in the MapReduce_UnixTools directory (see the README file in that directory for details).

## Java Version

The Java version can be run in cluster mode (via yarn/HDFS) or in local mode (local filesystem) using the following maven goals: -

Cluster mode
```bash
 mvn exec:exec@run-cluster 
```

Local mode
```bash
 mvn exec:exec@run-local 
```
		
