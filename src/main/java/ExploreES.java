import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.cache.recycler.PageCacheRecycler;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.logging.log4j.LogConfigurator;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.discovery.Discovery;
import org.elasticsearch.discovery.local.LocalDiscovery;
import org.elasticsearch.env.Environment;
import org.elasticsearch.http.HttpServer;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.http.netty.NettyHttpServerTransport;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.elasticsearch.node.internal.InternalSettingsPreparer;
import org.elasticsearch.node.service.NodeService;
import org.elasticsearch.node.settings.NodeSettingsService;
import org.elasticsearch.rest.*;
import org.elasticsearch.threadpool.ThreadPool;

/**
 * Created by arrawatia on 9/22/16.
 */
public class ExploreES {

    public static void main(String... args) throws InterruptedException {
        Runtime.getRuntime().traceMethodCalls(true);

        //        Node build = NodeBuilder.nodeBuilder().local(true).data(true).settings(Settings.builder()
//            .put(ClusterName.SETTING, "foobar")
//            .put("path.home", "/Users/arrawatia/code/omega/data")
//            // TODO: use a consistent data path for custom paths
//            // This needs to tie into the ESIntegTestCase#indexSettings() method
//            .put("path.shared_data", "/Users/arrawatia/code/omega")
//            .put("node.name", "foo")
//            .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
//            .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
//            .put("script.inline", "on")
//            .put("script.indexed", "on")
//            .put(EsExecutors.PROCESSORS, 1) // limit the number of threads created
//            .put("http.enabled", true)
//            .put(InternalSettingsPreparer.IGNORE_SYSTEM_PROPERTIES_SETTING, true) // make sure we get what we set :)
//        ).build();
//        build.start();
//        ClusterHealthResponse clusterHealthResponse = build.client().admin().cluster().prepareHealth().setWaitForGreenStatus().get();
          final ByteSizeValue BREAKER_LIMIT = new ByteSizeValue(20);

        Settings settings = Settings.builder()
            .put(HierarchyCircuitBreakerService.IN_FLIGHT_REQUESTS_CIRCUIT_BREAKER_LIMIT_SETTING, BREAKER_LIMIT)
            .put("path.logs", "/tmp/http")
            .put("cluster.name", "fubar")
            .put("path.home", "/Users/arrawatia/code/elasticsearch/distribution/src/main/resources")
            .build();
        LogConfigurator.configure(settings, true);
        CircuitBreakerService
            circuitBreakerService = new HierarchyCircuitBreakerService(settings, new NodeSettingsService(settings));
        // we can do this here only because we know that we don't adjust breaker settings dynamically in the test

//        CircuitBreaker inFlightRequestsBreaker =
//            circuitBreakerService.getBreaker(CircuitBreaker.IN_FLIGHT_REQUESTS);

//        PageCacheRecycler pg = new PageCacheRecycler(settings, new ThreadPool(settings));

        HttpServerTransport httpServerTransport = new NettyHttpServerTransport(settings, new NetworkService(settings), null);
        RestController restController = new RestController(settings);
        restController.registerHandler(RestRequest.Method.GET, "/", new RestHandler() {
            public void handleRequest(RestRequest request, RestChannel channel) throws Exception {
                System.out.println(request.headers());
//                new Thread(() -> {
//                    try {
////                        Thread.sleep(1000);
//                        channel.sendResponse(new BytesRestResponse(RestStatus.OK));
//                    } catch (InterruptedException e) {
//                        e.printStackTrace();
//                    }
//                });
                                        channel.sendResponse(new BytesRestResponse(RestStatus.OK));


            }

            public boolean canTripCircuitBreaker() {
                return true;
            }
        });
        restController.registerHandler(RestRequest.Method.GET, "/error", new RestHandler() {
            public void handleRequest(RestRequest request, RestChannel channel) throws Exception {
                throw new IllegalArgumentException("test error");
            }

            public boolean canTripCircuitBreaker() {
                return true;
            }
        });

        Discovery discovery = new LocalDiscovery(settings, null, null, null);
        NodeService
            nodeService = new NodeService(settings, null, null, discovery, null, null, null, null, null);
        
        HttpServer httpServer =
            new HttpServer(settings, new Environment(settings), httpServerTransport, restController,
                nodeService, circuitBreakerService);
        httpServer.start();

        System.out.println("HTTP SERVER STARTED !");
        while(true) {Thread.sleep(100000);}
    }


}
