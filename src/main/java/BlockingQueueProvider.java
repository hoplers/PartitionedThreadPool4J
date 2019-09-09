import java.util.concurrent.BlockingQueue;

@FunctionalInterface
public interface BlockingQueueProvider {

    BlockingQueue<Runnable> getBlockingQueue();

}
