package io.github.hoplers.partitionedthreadpoolexecutor;

import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class PartitionedThreadPoolExecutorTest {

    private PartitionedThreadPoolExecutor partitionedThreadPoolExecutor;

    private int numOfTasks = 1;

    private List<BlockingQueue> partitionQueues = new ArrayList<>();

    private BlockingQueueProvider blockingQueueProvider = () -> {
        LinkedBlockingQueue<Runnable> queue = new LinkedBlockingQueue<>();
        partitionQueues.add(queue);
        return queue;
    };

    private Semaphore firstPartitionSemaphore = new Semaphore(1);

    private Runnable firstPartitionRunnable = () -> {
        try {
            firstPartitionSemaphore.acquire();
            firstPartitionSemaphore.release();
        } catch (InterruptedException e) {
            fail("Unexpected interrupted exception");
        }
    };

    private Semaphore secondPartitionSemaphore = new Semaphore(1);

    private Runnable secondPartitionRunnable = () -> {
        try {
            secondPartitionSemaphore.acquire();
            secondPartitionSemaphore.release();
        } catch (InterruptedException e) {
            fail("Unexpected interrupted exception");
        }
    };

    @Before
    public void setup(){
        partitionedThreadPoolExecutor = new PartitionedThreadPoolExecutor(2, blockingQueueProvider , Executors.defaultThreadFactory(),(runnable, threadPoolExecutor) -> {},
                ((numOfPartitions, partitionKey) -> {
                    numOfTasks++;
                    return numOfTasks %numOfPartitions;
                }));
    }

    @Test
    public void testShutdownPartitionsFinishTheirTasksBeforeTemination() throws InterruptedException {
        firstPartitionSemaphore.acquire();
        secondPartitionSemaphore.acquire();
        partitionedThreadPoolExecutor.execute(firstPartitionRunnable,"a");
        partitionedThreadPoolExecutor.execute(secondPartitionRunnable,"a");
        partitionedThreadPoolExecutor.execute(firstPartitionRunnable,"a");
        partitionedThreadPoolExecutor.execute(secondPartitionRunnable,"a");
        assertEquals(1,partitionQueues.get(0).size());
        assertEquals(1,partitionQueues.get(1).size());
        partitionedThreadPoolExecutor.setPartitionCountAndShutdownRemovedPartitions(1);
        partitionedThreadPoolExecutor.execute(firstPartitionRunnable,"a");
        partitionedThreadPoolExecutor.execute(secondPartitionRunnable,"a");
        await().atMost(1, TimeUnit.SECONDS).untilAsserted(() -> assertEquals(3,partitionQueues.get(0).size()));
        await().atMost(1, TimeUnit.SECONDS).untilAsserted(() -> assertEquals(1,partitionQueues.get(1).size()));
        secondPartitionSemaphore.release();
        await().atMost(1, TimeUnit.SECONDS).untilAsserted(() -> assertEquals(0,partitionQueues.get(1).size()));
        firstPartitionSemaphore.release();
        await().atMost(1, TimeUnit.SECONDS).untilAsserted(() -> assertEquals(0,partitionQueues.get(0).size()));
    }

}