@FunctionalInterface
public interface Partitioner {
    int getPartitionForKey(int numOfPartitions, String partitionKey);
}
