# hudi-compaction-5298
The purpose is to reproduce the issue described in https://github.com/apache/hudi/issues/5298

## Incentive 
The issue was discovered first when running Hudi 0.9.0 on EMR 6.5.0. The ticket has been created that covers all the details 
and symptoms [HUDI-5298](https://github.com/apache/hudi/issues/5298). Please check that issue first to get more details, 
however in a nutshell the bullet point is that while we use multiple StreamingQuery(s) in one Spark Application 
with single/multiple SparkSession(s) and with single SparkContext we are having issues when Hudi inline/async compaction completes.
As per documentation of Spark, having multiple Streaming queries as well as multiple SparkSession is a valid and thread-safe usecase.
Each streaming query in our usecase consumes single topic from Kafka and use structured streaming to write to a single Hudi MOR table

After compaction completes, compaction commit file in _.hoodie_ folder references the parquet file 
which is deleted as part of that compaction by the logic in `HoodieTable#reconcileAgainstMarkers`. The issue manifests itself
in the form of FileNotFoundException when the reader using the Incremental query tries to read that commit file later

### Reproduce Locally (Hudi 0.9.0 and 0.10.0)
We were also able to reproduce the issue locally. The code in that repository gives a similar approximation to the use case 
we have in the EMR cluster. Multiple writes are initiated in its own thread, and writes to a single dedicated tables.

In the `com.example.hudi.HudiWriteChecker` you can find some descriptions on what Hudi options we have already tried

To initiate the test run the following command
```bash
rm -rf /tmp/hudi/input && sbt -J-Xmx3G -Dsandbox.path=/tmp/hudi/input -Dspark.cores=4 -Dspark.memory=2G -Dhudi.tables=4 run
```
One can change the Spark/Hudi version in _build.sbt_

The application will shutdown once the issue is detected. After that one can search for the logs like
>30196 [pool-28-thread-2] INFO  org.apache.hudi.table.HoodieTable  - Removing duplicate data files created due to spark retries before committing. Paths=[partition=partition_0/c3b9a931-4f4b-45e6-91ee-669aaaa43152-0_3-300-644_20220420193045.parquet, partition=partition_1/212e1ca2-4bb1-4352-8fa3-76f151450cae-0_2-300-643_20220420193045.parquet, partition=partition_3/d6a54049-ce68-4816-b8bd-e64496e15b5d-0_1-300-642_20220420193045.parquet, partition=partition_2/bb5cfcfc-eec6-40a8-8758-76958bbe7c62-0_0-300-641_20220420193045.parquet, partition=partition_4/0bde2d99-aba7-4f36-a96e-7584d8beee92-0_4-300-645_20220420193045.parquet]

>30440 [pool-28-thread-2] INFO  org.apache.hudi.client.SparkRDDWriteClient  - Committing Compaction 20220420193045. Finished with result HoodieCommitMetadata{partitionToWriteStats={partition=partition_4=[HoodieWriteStat{fileId='0bde2d99-aba7-4f36-a96e-7584d8beee92-0', path='partition=partition_4/0bde2d99-aba7-4f36-a96e-7584d8beee92-0_4-300-645_20220420193045.parquet', prevCommit='20220420193022', numWrites=1, numDeletes=0, numUpdateWrites=1, totalWriteBytes=436176, totalWriteErrors=0, tempPath='null', partitionPath='partition=partition_4', totalLogRecords=2, totalLogFilesCompacted=2, totalLogSizeCompacted=2642, totalUpdatedRecordsCompacted=1, totalLogBlocks=2, totalCorruptLogBlock=0, totalRollbackBlocks=0}], partition=partition_2=[HoodieWriteStat{fileId='bb5cfcfc-eec6-40a8-8758-76958bbe7c62-0', path='partition=partition_2/bb5cfcfc-eec6-40a8-8758-76958bbe7c62-0_0-300-641_20220420193045.parquet', prevCommit='20220420193022', numWrites=50, numDeletes=0, numUpdateWrites=50, totalWriteBytes=436634, totalWriteErrors=0, tempPath='null', partitionPath='partition=partition_2', totalLogRecords=100, totalLogFilesCompacted=2, totalLogSizeCompacted=17930, totalUpdatedRecordsCompacted=50, totalLogBlocks=2, totalCorruptLogBlock=0, totalRollbackBlocks=0}], partition=partition_3=[HoodieWriteStat{fileId='d6a54049-ce68-4816-b8bd-e64496e15b5d-0', path='partition=partition_3/d6a54049-ce68-4816-b8bd-e64496e15b5d-0_1-300-642_20220420193045.parquet', prevCommit='20220420193022', numWrites=50, numDeletes=0, numUpdateWrites=50, totalWriteBytes=436633, totalWriteErrors=0, tempPath='null', partitionPath='partition=partition_3', totalLogRecords=100, totalLogFilesCompacted=2, totalLogSizeCompacted=17930, totalUpdatedRecordsCompacted=50, totalLogBlocks=2, totalCorruptLogBlock=0, totalRollbackBlocks=0}], partition=partition_0=[HoodieWriteStat{fileId='c3b9a931-4f4b-45e6-91ee-669aaaa43152-0', path='partition=partition_0/c3b9a931-4f4b-45e6-91ee-669aaaa43152-0_3-300-644_20220420193045.parquet', prevCommit='20220420193022', numWrites=49, numDeletes=0, numUpdateWrites=49, totalWriteBytes=436614, totalWriteErrors=0, tempPath='null', partitionPath='partition=partition_0', totalLogRecords=98, totalLogFilesCompacted=2, totalLogSizeCompacted=17404, totalUpdatedRecordsCompacted=49, totalLogBlocks=2, totalCorruptLogBlock=0, totalRollbackBlocks=0}], partition=partition_1=[HoodieWriteStat{fileId='212e1ca2-4bb1-4352-8fa3-76f151450cae-0', path='partition=partition_1/212e1ca2-4bb1-4352-8fa3-76f151450cae-0_2-300-643_20220420193045.parquet', prevCommit='20220420193022', numWrites=50, numDeletes=0, numUpdateWrites=50, totalWriteBytes=436624, totalWriteErrors=0, tempPath='null', partitionPath='partition=partition_1', totalLogRecords=100,

>Caused by: java.io.FileNotFoundException: File file:/tmp/hudi/input/test_table_2/partition=partition_1/212e1ca2-4bb1-4352-8fa3-76f151450cae-0_2-300-643_20220420193045.parquet does not exist
at org.apache.hadoop.fs.RawLocalFileSystem.deprecatedGetFileStatus(RawLocalFileSystem.java:666)
at org.apache.hadoop.fs.RawLocalFileSystem.getFileLinkStatusInternal(RawLocalFileSystem.java:987)

The reproduction rate is close to 1 and we have been able to reproduce it on Mac and Linux. On my laptop with the configuration above I was able to reproduce the issue 7 times out of 10 
However, sometimes you might hit OOM - in that case kill the running process and start the command again

One can play around with `-Dhudi.tables=4` which uses that number of threads and Hudi tables (thread per table)

I could not reproduce the issue using a single table, neither in EMR nor in local set-up

Note: There seems to be a patch available for a similar issue [here](https://github.com/apache/hudi/pull/4753), however
we have not tried it yet