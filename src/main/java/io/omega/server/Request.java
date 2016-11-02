package io.omega.server;


import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Protocol;
import org.apache.kafka.common.protocol.SecurityProtocol;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.ApiVersionsRequest;
import org.apache.kafka.common.requests.RequestHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Objects;

import io.omega.network.Session;

public class Request {

    private static final Logger log = LoggerFactory.getLogger(Request.class);

    private final short requestId;
    private final Integer processor;
    private final SecurityProtocol securityProtocol;
    private final RequestHeader header;
    private final String connectionId;
    private final AbstractRequest body;
    private final ByteBuffer buffer;
    volatile Long requestDequeueTimeMs = -1L;
    volatile Long apiLocalCompleteTimeMs = -1L;
    volatile Long responseCompleteTimeMs = -1L;
    volatile Long responseDequeueTimeMs = -1L;
    volatile Long apiRemoteCompleteTimeMs = -1L;

    public Request(Integer processor, String connectionId, Session session, ByteBuffer buffer, Long startTimeMs, SecurityProtocol securityProtocol) {
        // These need to be volatile because the readers are in the network thread and the writers are in the request
        // handler threads or the purgatory threads
        this.requestId = buffer.getShort();
        this.buffer = buffer;
        this.securityProtocol = securityProtocol;
        this.processor = processor;
        this.connectionId = connectionId;

        // Read the buffer.
        buffer.rewind();
        this.header = RequestHeader.parse(buffer);
        this.body = parseBody(buffer);
    }

    private AbstractRequest parseBody(ByteBuffer buffer) {
        try {
            // For unsupported version of ApiVersionsRequest, create a dummy request to enable an error response to be returned later
            if (header.apiKey() == ApiKeys.API_VERSIONS.id && !Protocol.apiVersionSupported(header.apiKey(), header.apiVersion()))
                return new ApiVersionsRequest();
            else
                return AbstractRequest.getRequest(header.apiKey(), header.apiVersion(), buffer);
        } catch (Throwable e) {
            throw new RuntimeException("Error getting request for apiKey:" + header.apiKey() + " and apiVersion: " + header.apiVersion(), e);
        }
    }

    public int processor() {
        return this.processor;
    }
//        // TODO: this will be removed once we migrated to client-side format
//        // for server-side request / response format
//        // NOTE: this map only includes the server-side request/response handlers. Newer
//        // request types should only use the client-side versions which are parsed with
//        // o.a.k.common.requests.AbstractRequest.getRequest()
//        private val keyToNameAndDeserializerMap: Map[Short, (ByteBuffer) =>
//        HandlerProcessor.RequestOrResponse]=
//        Map(ApiKeys.FETCH.id -> FetchRequest.readFrom,
//                ApiKeys.CONTROLLED_SHUTDOWN_KEY.id -> ControlledShutdownRequest.readFrom
//      )
//
//        // TODO: this will be removed once we migrated to client-side format
//        val requestObj =
//                keyToNameAndDeserializerMap.get(requestId).map(readFrom => readFrom(buffer)).orNull
//
//        // if we failed to find a server-side mapping, then try using the
//        // client-side request / response format
//        val header: RequestHeader =
//        if (requestObj == null) {
//            buffer.rewind
//            try RequestHeader.parse(buffer)
//        catch {
//                case ex: Throwable =>
//                    throw new InvalidRequestException(s"Error parsing request header. Our best guess of the apiKey is: $requestId", ex)
//            }
//        } else
//            null
//        val body: AbstractRequest =
//        if (requestObj == null)
//            try {
//                // For unsupported version of ApiVersionsRequest, create a dummy request to enable an error response to be returned later
//                if (header.apiKey == ApiKeys.API_VERSIONS.id && !Protocol.apiVersionSupported(header.apiKey, header.apiVersion))
//                    new ApiVersionsRequest
//                else
//                    AbstractRequest.getRequest(header.apiKey, header.apiVersion, buffer)
//            } catch {
//            case ex: Throwable =>
//                throw new InvalidRequestException(s"Error getting request for apiKey: ${header.apiKey} and apiVersion: ${header.apiVersion}", ex)
//        }
//      else
//        null
//
//        buffer = null
//        private val requestLogger = Logger.getLogger("kafka.request.logger")
//
//        def requestDesc(details: Boolean): String = {
//        if (requestObj != null)
//            requestObj.describe(details)
//        else
//            header.toString + " -- " + body.toString
//    }

//        trace("Processor %d received request : %s".format(processor, requestDesc(true)))

//        def updateRequestMetrics() {
//            val endTimeMs = SystemTime.milliseconds
//            // In some corner cases, apiLocalCompleteTimeMs may not be set when the request completes if the remote
//            // processing time is really small. This value is set in KafkaApis from a request handling thread.
//            // This may be read in a network thread before the actual update happens in KafkaApis which will cause us to
//            // see a negative value here. In that case, use responseCompleteTimeMs as apiLocalCompleteTimeMs.
//            if (apiLocalCompleteTimeMs < 0)
//                apiLocalCompleteTimeMs = responseCompleteTimeMs
//            // If the apiRemoteCompleteTimeMs is not set (i.e., for requests that do not go through a purgatory), then it is
//            // the same as responseCompleteTimeMs.
//            if (apiRemoteCompleteTimeMs < 0)
//                apiRemoteCompleteTimeMs = responseCompleteTimeMs
//
//            val requestQueueTime = math.max(requestDequeueTimeMs - startTimeMs, 0)
//            val apiLocalTime = math.max(apiLocalCompleteTimeMs - requestDequeueTimeMs, 0)
//            val apiRemoteTime = math.max(apiRemoteCompleteTimeMs - apiLocalCompleteTimeMs, 0)
//            val apiThrottleTime = math.max(responseCompleteTimeMs - apiRemoteCompleteTimeMs, 0)
//            val responseQueueTime = math.max(responseDequeueTimeMs - responseCompleteTimeMs, 0)
//            val responseSendTime = math.max(endTimeMs - responseDequeueTimeMs, 0)
//            val totalTime = endTimeMs - startTimeMs
//            val fetchMetricNames =
//            if (requestId == ApiKeys.FETCH.id) {
//                val isFromFollower = requestObj.asInstanceOf[FetchRequest].isFromFollower
//                Seq(
//                if (isFromFollower) RequestMetrics.followFetchMetricName
//                else RequestMetrics.consumerFetchMetricName
//          )
//            }
//            else Seq.empty
//            val metricNames = fetchMetricNames :+ ApiKeys.forId(requestId).name
//            metricNames.foreach { metricName =>
//                val m = RequestMetrics.metricsMap(metricName)
//                m.requestRate.mark()
//                m.requestQueueTimeHist.update(requestQueueTime)
//                m.localTimeHist.update(apiLocalTime)
//                m.remoteTimeHist.update(apiRemoteTime)
//                m.throttleTimeHist.update(apiThrottleTime)
//                m.responseQueueTimeHist.update(responseQueueTime)
//                m.responseSendTimeHist.update(responseSendTime)
//                m.totalTimeHist.update(totalTime)
//            }
//
//            if (requestLogger.isTraceEnabled)
//                requestLogger.trace("Completed request:%s from connection %s;totalTime:%d,requestQueueTime:%d,localTime:%d,remoteTime:%d,responseQueueTime:%d,sendTime:%d,securityProtocol:%s,
// principal:%s"
//                        .format(requestDesc(true), connectionId, totalTime, requestQueueTime, apiLocalTime, apiRemoteTime, responseQueueTime, responseSendTime, securityProtocol, session.principal))
//            else if (requestLogger.isDebugEnabled)
//                requestLogger.debug("Completed request:%s from connection %s;totalTime:%d,requestQueueTime:%d,localTime:%d,remoteTime:%d,responseQueueTime:%d,sendTime:%d,securityProtocol:%s,
// principal:%s"
//                        .format(requestDesc(false), connectionId, totalTime, requestQueueTime, apiLocalTime, apiRemoteTime, responseQueueTime, responseSendTime, securityProtocol, session.principal))
//        }

    public short requestId() {
        return requestId;
    }

    public SecurityProtocol securityProtocol() {
        return securityProtocol;
    }

    public String connectionId() {
        return connectionId;
    }

    public RequestHeader header() {
        return header;
    }

    public AbstractRequest body() {
        return body;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Request request = (Request) o;
        return requestId == request.requestId &&
                Objects.equals(processor, request.processor) &&
                securityProtocol == request.securityProtocol &&
                Objects.equals(header, request.header) &&
                Objects.equals(connectionId, request.connectionId) &&
                Objects.equals(body, request.body) &&
                Objects.equals(buffer, request.buffer);
    }

    @Override
    public int hashCode() {
        return Objects.hash(requestId, processor, securityProtocol, header, connectionId, body, buffer);
    }

    @Override
    public String toString() {
        return "Request{" +
                "\nrequestId=" + requestId +
                ", \ntype=" + ApiKeys.forId(header.apiKey()) +
                ", \nprocessor=" + processor +
                ", \nsecurityProtocol=" + securityProtocol +
                ", \nconnectionId='" + connectionId + '\'' +
                ", \nheader=" + header +
                ", \nbody=" + body +
                '}';
    }
}