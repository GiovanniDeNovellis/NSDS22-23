package it.polimi.middleware.kafka.backend.Services;


import com.google.gson.Gson;
import it.polimi.middleware.kafka.backend.Entities.MessageWrapper;
import it.polimi.middleware.kafka.backend.Entities.Order;

import javax.websocket.OnMessage;
import javax.websocket.OnOpen;
import javax.websocket.Session;
import javax.websocket.server.PathParam;
import javax.websocket.server.ServerEndpoint;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.CopyOnWriteArraySet;

@ServerEndpoint("/orderservice/{email}")
public class OrderServerEndpoint {
    private Session session;
    private static Set<OrderServerEndpoint> chatEndpoints= new CopyOnWriteArraySet<>();
    private static HashMap<String, String> users = new HashMap<>();

    @OnOpen
    public void onCreateSession(Session session, @PathParam("email") String username){
        chatEndpoints.add(this);
        users.put(session.getId(), username);
        this.session = session;
        System.out.println("new user connected to orders: "+users.get(session.getId()));
    }

    @OnMessage
    public void onRequest(String message){
        String[] arr = message.split(" ");
        Gson gson = new Gson();
        switch (arr[0]) {
            case "update_items": {
                System.out.println("message = " + message);
                OrderWebSocketServer.ordersService.addItem(arr[1], Integer.parseInt(arr[2]));
                MessageWrapper toSend = new MessageWrapper("update_items", "success");
                String serializedMessage = gson.toJson(toSend);
                sendMessage(serializedMessage);
                System.out.println(serializedMessage);
                break;
            }
            case "items": {
                Map<String, Integer> AvailableItems = OrderWebSocketServer.ordersService.getItemAvailability();
                String serializedItemList = gson.toJson(AvailableItems);
                MessageWrapper toSend = new MessageWrapper("items", serializedItemList);
                String serializedMessage = gson.toJson(toSend);
                sendMessage(serializedMessage);
                System.out.println(serializedMessage);
                break;
            }
            case "order": {
                Map<String, Integer> ItemsToOrder = new HashMap<>();
                for (int i = 1; i < arr.length - 1; i = i + 2) {
                    ItemsToOrder.put(arr[i], Integer.parseInt(arr[i + 1]));
                }
                String result = OrderWebSocketServer.ordersService.placeOrder(ItemsToOrder, users.get(session.getId()), arr[arr.length - 1]);
                MessageWrapper toSend = new MessageWrapper("order", result);
                String serializedMessage = gson.toJson(toSend);
                sendMessage(serializedMessage);
                System.out.println(serializedMessage);
                break;
            }
            case "history": {
                List<Order> MyOrders = OrderWebSocketServer.ordersService.checkMyOrders(users.get(session.getId()));
                String serializedOrderList = gson.toJson(MyOrders);
                MessageWrapper toSend = new MessageWrapper("history", serializedOrderList);
                String serializedMessage = gson.toJson(toSend);
                sendMessage(serializedMessage);
                System.out.println(serializedMessage);
                break;
            }
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
