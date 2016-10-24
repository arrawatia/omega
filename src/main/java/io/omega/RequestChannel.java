package io.omega;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.SecurityProtocol;
import org.apache.kafka.common.requests.ProduceRequest;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.RequestSend;
import org.apache.kafka.common.security.auth.KafkaPrincipal;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;


public class RequestChannel {
    private final ArrayBlockingQueue<Request> requestQueue;
    private final Request AllDone;
    List<ResponseListener> responseListeners;
    BlockingQueue<Response>[] responseQueues;

    public RequestChannel(Integer numProcessors, Integer queueSize) {
        this.responseListeners = null;
        this.requestQueue = new ArrayBlockingQueue<>(queueSize);
        this.responseQueues = new BlockingQueue[numProcessors];
        for (int i = 0; i < numProcessors; i++) {

            responseQueues[i] = new LinkedBlockingQueue<>();


        }

        Session allDoneSession = null;
        try {
            allDoneSession = new Session(KafkaPrincipal.ANONYMOUS, InetAddress.getLocalHost());
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        this.AllDone = new Request(1, "2", allDoneSession, getShutdownReceive(), (long) 0,
                SecurityProtocol.PLAINTEXT);
//        for(i )
//
//        newGauge(
//                "RequestQueueSize",
//                new Gauge[Int] {
//            def value = requestQueue.size
//        }
//  )

//        newGauge("ResponseQueueSize", new Gauge[Int]{
//            def value = responseQueues.foldLeft(0) {(total, q) => total + q.size()}
//        })

//        for (i <- 0 until numProcessors) {
//            newGauge("ResponseQueueSize",
//                    new Gauge[Int] {
//                def value = responseQueues(i).size()
//            },
//            Map("processor" -> i.toString)
//    )
    }

    /**
     * Send a request to be handled, potentially blocking until there is room in the queue
     * for the
     * request
     */
    public void sendRequest(Request request) {
        this.requestQueue.add(request);
    }

    /** Send a response back to the socket server to be sent over the network */
    public void sendResponse(Response response) {
        responseQueues[response.processor()].add(response);
        for (ResponseListener listener : this.responseListeners) {
            listener.onResponse(response.processor());
        }
    }

    /** No operation to take for the request, need to read more over the network */
    public void noOperation(Integer processor, Request request) {
        this.responseQueues[processor].add(new Response(processor, request, null, Response
                .ResponseAction.NOOP));
        for (ResponseListener listener : responseListeners)
            listener.onResponse(processor);
    }

    /** Close the connection for the request */
    public void closeConnection(Integer processor, Request request) {
        responseQueues[processor].add(new Response(processor, request, null,
                Response.ResponseAction.CLOSE));
        for (ResponseListener listener : responseListeners)
            listener.onResponse(processor);
    }

    /** Get the next request or block until specified time has elapsed */
    public Request receiveRequest(Long timeout) {
        try {
            return requestQueue.poll(timeout, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            return null;
        }
    }

    /** Get the next request or block until there is one */
    public Request receiveRequest() {
        try {
            return requestQueue.take();
        } catch (InterruptedException e) {
            return null;
        }
    }

    /** Get a response for the given processor if there is one */
    public Response receiveResponse(Integer processor) {
        Response response = responseQueues[processor].poll();
//        if (response != null)
//            response.request().responseDequeueTimeMs() = SystemTime.milliseconds();
        return response;
    }

    public void addResponseListener(ResponseListener onResponse) {
        responseListeners.add(onResponse);
    }

    public void shutdown() {
        requestQueue.clear();
    }

    public ByteBuffer getShutdownReceive() {
        RequestHeader emptyRequestHeader = new RequestHeader(ApiKeys.PRODUCE.id, "", 0);
        ProduceRequest emptyProduceRequest = new ProduceRequest((short) 0, 0, new
                HashMap<TopicPartition, ByteBuffer>());
        return RequestSend.serialize(emptyRequestHeader, emptyProduceRequest.toStruct());
    }
}