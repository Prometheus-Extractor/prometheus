# Working with Neural Nets
*Later versions of Prometheus uses DL4J to do deep learning. This is not entirely uncomplicated.*

## DL4J Quirks

The biggest problem with DL4J is the memory issues with JavaCPP, since it uses off-heap memory and seems to use more than it advertises.

To make it work you need to set:
- `org.bytedeco.javacpp.maxbytes` to around minimum `5g`. Though more is required for larger batch sizes and larger networks.
- `org.bytedeco.javacpp.maxphysicalbytes` about the maximum memory usable by the JVM (heap + off heap).
- Set the executor's heap to about 6gb during training.

This works fairly well for our networks during training. However stages that require the Word2vec model to be loaded,
such as feature transformation or prediction, require about 6gb just for the model. So during those stages the heap
needs to be at least 10gb.

Therefore the recommended memory size is around `30gb`. If you don't have that. Run the different stages in the pipeline
with varying size for the heap and the off-heap memory.

Also note that you CANNOT persist off-heap memory with the `_SER`-flag.

## Network Optimization
- More advanced updaters such as ADADELTA seem to outperform NESTROV.
- Running without partitioning between iterations speeds up but might cause huge results.

## AWS
When running on AWS EMR. Yarn requires some non-trivial configuration to work optimally.

- http://blog.cloudera.com/blog/2015/03/how-to-tune-your-apache-spark-jobs-part-2/
