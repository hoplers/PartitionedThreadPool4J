package io.github.hoplers.partitionedthreadpoolexecutor;

import java.util.concurrent.BlockingQueue;

@FunctionalInterface
public interface BlockingQueueProvider {

    BlockingQueue<Runnable> getBlockingQueue();

}
