package company.vk.edu.distrib.compute.nesterukia;

import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.KVServiceFactory;

import java.io.IOException;

public class InMemoryKVServiceFactory extends KVServiceFactory {
    @Override
    protected KVService doCreate(int port) throws IOException {
        return new KVServiceImpl(
                port,
                new InMemoryKVDao()
        );
    }
}
