package it.polimi.middleware.kafka.backend.Entities;

public class MessageWrapper {
    private String type;
    private String payload;

    public MessageWrapper(String type, String payload) {
        this.type = type;
        this.payload = payload;
    }

    @Override
    public String toString() {
        return "MessageWrapper [type=" + type + ", payload=" + payload + "]";
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getType() {
        return type;
    }

    public String getPayload() {
        return payload;
    }

    public void setPayload(String payload) {
        this.payload = payload;
    }

}
