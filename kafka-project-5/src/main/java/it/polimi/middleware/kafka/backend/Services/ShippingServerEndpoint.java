package it.polimi.middleware.kafka.backend.Services;

import com.google.gson.Gson;
import it.polimi.middleware.kafka.backend.Entities.MessageWrapper;
import it.polimi.middleware.kafka.backend.Entities.Order;

import javax.websocket.OnMessage;
import javax.websocket.OnOpen;
import javax.websocket.Session;
import javax.websocket.server.ServerEndpoint;
import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

@ServerEndpoint("/shippingservice")
public class ShippingServerEndpoint {
    private Session session;

    private static Set<ShippingServerEndpoint> chatEndpoints= new CopyOnWriteArraySet<>();

    @OnOpen
    public void onCreateSession(Session session){
        chatEndpoints.add(this);
        this.session = session;
    }

    @OnMessage
    public void onRequest(String message){
        String arr[] = message.split(" ");
        Gson gson = new Gson();

        if(arr[0].equals("update_order")){
            System.out.println("ID to update: " + Integer.parseInt(arr[1]));
            String result = ShippingWebSockerServer.shippingService.updateOrder("Delivered", Integer.parseInt(arr[1]));
            MessageWrapper toSend = new MessageWrapper("update_order", result);
            String serializedMessage = gson.toJson(toSend);
            sendMessage(serializedMessage);
            System.out.println("Sending " + serializedMessage);
        } else if (arr[0].equals("show_deliveries")) {
            List<Order> deliveries = ShippingWebSockerServer.shippingService.getOngoingDeliveries();
            String serializedOrdersList = gson.toJson(deliveries);
            MessageWrapper toSend = new MessageWrapper("deliveries",serializedOrdersList);
            String serializedMessage = gson.toJson(toSend);
            sendMessage(serializedMessage);
            System.out.println("Sending " + serializedMessage);
        }
    }

    private void sendMessage(String message){
        if(this.session != null && this.session.isOpen()){
            try {
                this.session.getBasicRemote().sendText(message);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
