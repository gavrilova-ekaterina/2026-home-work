package company.vk.edu.distrib.compute.gavrilova_ekaterina.sharding;

import java.util.List;

public interface HashingStrategy {

    void setEndpoints(List<String> endpoints);

    String getNode(String key);

}
