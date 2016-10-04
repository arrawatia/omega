package io.omega;

import org.elasticsearch.common.component.LifecycleComponent;

/**
 * Created by arrawatia on 10/2/16.
 */
// Lifecycle component
public class KafkaServer {

//    var apis: KafkaApis = null
//    var authorizer: Option[Authorizer] = None
//    var socketServer: SocketServer = null
//    var requestHandlerPool: KafkaRequestHandlerPool = null
//    var zkUtils: ZkUtils = null
//    var metadataCache: MetadataCache = null
//a
//
//    val correlationId: AtomicInteger = new AtomicInteger(0)
//
//    metadataCache = new MetadataCache(config.brokerId)
//    socketServer = new SocketServer(config, metrics, kafkaMetricsTime)
//    socketServer.startup()
//
//            /* Get the authorizer and initialize it if one is specified.*/
//    authorizer = Option(config.authorizerClassName).filter(_.nonEmpty).map { authorizerClassName =>
//        val authZ = CoreUtils.createObject[Authorizer](authorizerClassName)
//            authZ.configure(config.originals())
//        authZ
//    }
//
//     /* start processing requests */
//    apis = new KafkaApis(socketServer.requestChannel, replicaManager, adminManager, groupCoordinator,
//        kafkaController, zkUtils, config.brokerId, config, metadataCache, metrics, authorizer)
//    requestHandlerPool = new KafkaRequestHandlerPool(config.brokerId, socketServer.requestChannel, apis, config.numIoThreads)
//
//    /* tell everyone we are alive */
//    val listeners = config.advertisedListeners.map {case(protocol, endpoint) =>
//        if (endpoint.port == 0)
//            (protocol, EndPoint(endpoint.host, socketServer.boundPort(protocol), endpoint.protocolType))
//        else
//        (protocol, endpoint)
//    }



}
