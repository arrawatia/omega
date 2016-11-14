package io.omega.proxy;

import io.omega.ProxyServerConfig;

/**
 * Created by sumit on 11/1/16.
 */
public class RequestDispatcherFactory {

    public static KafkaRequestDispatcher create(ProxyServerConfig cfg){
        KafkaRequestDispatcher dis = new KafkaApis(cfg);
        MetadataCache mc = new MetadataCache(cfg);
        new MetadataHandler(dis, mc, cfg);
        new FetchHandler(dis, mc);
        new ProduceHandler(dis, mc);
        new GroupCoordinatorHandler(dis, mc, cfg);
        new ListOffsetsHandler(dis, mc);
        new CoordinatorRequestHandler(dis, mc);
        new ControllerRequestHandler(dis, mc);
        new ApiVersionsHandler(dis, mc);
        return dis;
    }
}
