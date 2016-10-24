package io.omega;

import org.apache.kafka.common.network.Send;
import org.apache.kafka.common.utils.SystemTime;

public class Response {

    private final Request request;
    private final ResponseAction responseAction;
    private final Integer processor;
    private final Send responseSend;

    public enum ResponseAction {NOOP, SEND, CLOSE}

    public ResponseAction responseAction() {
        return this.responseAction;
    }

    public Request request() {
        return this.request;
    }

    public int processor(){
        return this.processor;
    }

    public Send responseSend(){
        return this.responseSend;
    }


    public Response(Integer processor, Request request, Send responseSend , ResponseAction responseAction ) {
//        request.responseCompleteTimeMs() = SystemTime.milliseconds();
        this.request = request;
        this.responseAction = responseAction;
        this.processor = processor;
        this.responseSend = responseSend;
    }

    public Response(Integer processor, Request request, Send responseSend ) {
            this(processor, request, responseSend, responseSend == null ? ResponseAction.NOOP : ResponseAction.SEND);
    }

    public Response(Request request, Send send){
            this(request.processor(), request, send);
    }
}