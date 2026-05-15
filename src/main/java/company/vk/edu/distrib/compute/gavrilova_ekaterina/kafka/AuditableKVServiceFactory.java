package company.vk.edu.distrib.compute.gavrilova_ekaterina.kafka;

import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.KVServiceFactory;

import java.io.IOException;

public class AuditableKVServiceFactory extends KVServiceFactory {

    @Override
    protected KVService doCreate(int port) throws IOException {
        return new AuditableKVServiceImpl(port);
    }
}
