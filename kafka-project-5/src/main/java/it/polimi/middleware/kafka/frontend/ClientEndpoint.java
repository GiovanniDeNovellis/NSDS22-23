package it.polimi.middleware.kafka.frontend;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import it.polimi.middleware.kafka.backend.Entities.MessageWrapper;
import it.polimi.middleware.kafka.backend.Entities.Order;
import it.polimi.middleware.kafka.backend.Entities.User;

import javax.websocket.Endpoint;
import javax.websocket.EndpointConfig;
import javax.websocket.MessageHandler;
import javax.websocket.Session;
import java.io.IOException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ClientEndpoint extends Endpoint {
    private Session session;
    public User myUser = null;

    @Override
    public void onOpen(Session session, EndpointConfig config){
        this.session = session;

        this.session.addMessageHandler(new MessageHandler.Whole<String>() {
            @Override
            public void onMessage(String message){
                System.out.println("Received message: " + message);
                Gson gson = new Gson();
                MessageWrapper receivedMessage = gson.fromJson(message, MessageWrapper.class);

                switch (receivedMessage.getType()) {
                    case "register": {
                        User user = gson.fromJson(receivedMessage.getPayload(), User.class);
                        if (user != null) {
                            myUser = user;
                            System.out.println("Successful registration.");
                        } else {
                            System.out.println("Unsuccessful registration. " +
                                    "You probably have already registered with this email address.");
                        }
                        break;
                    }
                    case "login": {
                        User user = gson.fromJson(receivedMessage.getPayload(), User.class);
                        if (user != null) {
                            myUser = user;
                            System.out.println("Successful login.");
                        } else {
                            System.out.println("Unsuccessful login, invalid credentials.");
                        }
                        break;
                    }
                    case "update_items":
                        System.out.println("items updated with " + receivedMessage.getPayload() + "!");
                        break;
                    case "items":
                        Map<String, Integer> AvailableItems = gson.fromJson(receivedMessage.getPayload(), Map.class);
                        for (String key : AvailableItems.keySet()) {
                            System.out.println(key + ": " + AvailableItems.get(key));
                        }
                        break;
                    case "order":
                        System.out.println(receivedMessage.getPayload() + "!");
                        break;
                    case "history":
                        System.out.println("Here is the list of orders you bought: ");
                        Type ListType = new TypeToken<ArrayList<Order>>() {
                        }.getType();
                        List<Order> boughtOrderList = gson.fromJson(receivedMessage.getPayload(), ListType);
                        for (Order ord : boughtOrderList)
                            System.out.println(ord);
                        break;
                    case "deliveries":
                        System.out.println("Here is the list of orders yet to be delivered: ");
                        Type ordersListType = new TypeToken<ArrayList<Order>>(){}.getType();
                        List<Order> orderList = gson.fromJson(receivedMessage.getPayload(), ordersListType);
                        for(Order ord: orderList)
                            System.out.println(ord);
                        break;
                 }
            }
        });
    }

    public void sendMessage(String message) throws IOException {
        this.session.getBasicRemote().sendText(message);
    }

}
