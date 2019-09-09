import java.util.Collection;
import java.util.List;
import java.util.concurrent.*;

public interface PartitionedExecutor {

    void execute(Runnable runnable, String partitionKey);

    Future<?> submit(Runnable runnable, String partitionKey);

    <T> Future<T> submit(Callable<T> callable, String partitionKey);

    <T> Future<T> submit(Runnable runnable, T result, String partitionKey);

    List<Runnable> shutdownNow();

    boolean isShutdown();

    boolean isTerminated();

    boolean awaitTermination(long timeout, TimeUnit timeUnit) throws InterruptedException;

}
