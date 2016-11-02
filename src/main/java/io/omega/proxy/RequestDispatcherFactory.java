package io.omega.proxy;

import java.util.Map;

/**
 * Created by sumit on 11/1/16.
 */
public class RequestDispatcherFactory {

    public static KafkaRequestDispatcher create(Map<String, String> cfg){
        KafkaRequestDispatcher dis = new KafkaApis(cfg);
        MetadataCache mc = new MetadataCache();
        new MetadataHandler(dis, mc);
        new FetchHandler(dis, mc);
        new ProduceHandler(dis, mc);
        return dis;
    }
}
