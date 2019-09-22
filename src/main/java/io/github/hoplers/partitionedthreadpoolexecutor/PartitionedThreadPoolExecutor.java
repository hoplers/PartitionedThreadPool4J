package io.github.hoplers.partitionedthreadpoolexecutor;

import lombok.NonNull;

import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class PartitionedThreadPoolExecutor implements PartitionedExecutor {

    private static final ThreadPoolExecutor.AbortPolicy defaultHandler = new ThreadPoolExecutor.AbortPolicy();
    private final static Partitioner defaultPartitioner = (numOfPartitions, partitionKey) -> partitionKey.hashCode() % numOfPartitions;

    private final int numOfPartitions;
    private final BlockingQueueProvider blockingQueueProvider;
    private final ThreadFactory threadFactory;
    private final RejectedExecutionHandler rejectedExecutionHandler;
    private final Partitioner partitioner;

    private final Map<Integer, ThreadPoolExecutor> partitionsToThreadPoolExecutors;

    public PartitionedThreadPoolExecutor(int numOfPartitions){
        this(numOfPartitions, LinkedBlockingQueue::new);
    }

    public PartitionedThreadPoolExecutor(int numOfPartitions, BlockingQueueProvider blockingQueueProvider){
        this(numOfPartitions, blockingQueueProvider, Executors.defaultThreadFactory());
    }

    public PartitionedThreadPoolExecutor(int numOfPartitions, BlockingQueueProvider blockingQueueProvider, ThreadFactory threadFactory){
        this(numOfPartitions,blockingQueueProvider,threadFactory,defaultHandler,defaultPartitioner);
    }

    public PartitionedThreadPoolExecutor(int numOfPartitions, BlockingQueueProvider blockingQueueProvider, RejectedExecutionHandler rejectedExecutionHandler){
        this(numOfPartitions,blockingQueueProvider,Executors.defaultThreadFactory(),rejectedExecutionHandler,defaultPartitioner);
    }

    public PartitionedThreadPoolExecutor(int numOfPartitions, BlockingQueueProvider blockingQueueProvider,
                                         ThreadFactory threadFactory, RejectedExecutionHandler rejectedExecutionHandler,
                                         Partitioner partitioner){

        this.numOfPartitions = numOfPartitions;
        this.blockingQueueProvider = blockingQueueProvider;
        this.threadFactory = threadFactory;
        this.rejectedExecutionHandler = rejectedExecutionHandler;
        this.partitioner = partitioner;
        partitionsToThreadPoolExecutors = new HashMap<>();

    }

    @Override
    public void execute(@NonNull Runnable runnable, @NonNull String partitionKey) {
        getPartitionThreadPoolExecutor(partitionKey).execute(runnable);
    }

    @Override
    public Future<?> submit(@NonNull Runnable runnable, @NonNull String partitionKey) {
        return getPartitionThreadPoolExecutor(partitionKey).submit(runnable);
    }

    @Override
    public <T> Future<T> submit(@NonNull Callable<T> callable, @NonNull String partitionKey) {
        return getPartitionThreadPoolExecutor(partitionKey).submit(callable);
    }

    @Override
    public <T> Future<T> submit(@NonNull Runnable runnable, @NonNull T result, @NonNull String partitionKey) {
        return getPartitionThreadPoolExecutor(partitionKey).submit(runnable,result);
    }

    @Override
    public List<Runnable> shutdownNow() {
        return partitionsToThreadPoolExecutors.values().stream()
                .map(ThreadPoolExecutor::shutdownNow)
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
    }

    @Override
    public boolean isShutdown() {
        return partitionsToThreadPoolExecutors.values().stream().allMatch(ThreadPoolExecutor::isShutdown);
    }

    @Override
    public boolean isTerminated() {
        return partitionsToThreadPoolExecutors.values().stream().allMatch(ThreadPoolExecutor::isTerminated);
    }

    @Override
    public boolean awaitTermination(long totalTimeout, TimeUnit timeUnit) throws InterruptedException {
        long totalNanos = timeUnit.toNanos(totalTimeout);
        long nanosPerPartition = totalNanos / numOfPartitions;
        for (ThreadPoolExecutor partitionThreadPoolExecutor : partitionsToThreadPoolExecutors.values()){
           if (!partitionThreadPoolExecutor.awaitTermination(nanosPerPartition,TimeUnit.NANOSECONDS)){
               return false;
           }
        }
        return true;
    }

    private ThreadPoolExecutor getPartitionThreadPoolExecutor(String partitionKey) {
        return partitionsToThreadPoolExecutors.computeIfAbsent(
                        partitioner.getPartitionForKey(numOfPartitions, partitionKey),
                        (numOfPartitions) -> createNewPartition());
    }

    private ThreadPoolExecutor createNewPartition() {
        return new ThreadPoolExecutor(1,1,0,TimeUnit.MILLISECONDS,blockingQueueProvider.getBlockingQueue(),threadFactory,rejectedExecutionHandler);
    }

}
