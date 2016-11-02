package io.omega.server;

import org.apache.kafka.common.network.Send;

public class Response {

    public enum ResponseAction {NOOP, SEND, CLOSE}

    private final Request request;
    private final ResponseAction responseAction;
    private final Integer processor;
    private final Send responseSend;

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

    @Override
    public String toString() {
        return "Response{" +
                "request=" + request +
                ", responseAction=" + responseAction +
                ", processor=" + processor +
                ", responseSend=" + responseSend +
                '}';
    }
}