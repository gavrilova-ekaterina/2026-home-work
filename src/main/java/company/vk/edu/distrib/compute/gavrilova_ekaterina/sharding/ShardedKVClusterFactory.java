package company.vk.edu.distrib.compute.gavrilova_ekaterina.sharding;

import company.vk.edu.distrib.compute.KVCluster;
import company.vk.edu.distrib.compute.KVClusterFactory;

import java.util.List;

public class ShardedKVClusterFactory extends KVClusterFactory {

    @Override
    protected KVCluster doCreate(List<Integer> ports) {
        return new ShardedKVCluster(ports);
    }

}
