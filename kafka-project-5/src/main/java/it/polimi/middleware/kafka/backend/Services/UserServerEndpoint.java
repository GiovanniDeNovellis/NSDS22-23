package it.polimi.middleware.kafka.backend.Services;

import com.google.gson.Gson;
import it.polimi.middleware.kafka.backend.Entities.MessageWrapper;
import it.polimi.middleware.kafka.backend.Entities.User;

import javax.websocket.OnMessage;
import javax.websocket.OnOpen;
import javax.websocket.Session;
import javax.websocket.server.PathParam;
import javax.websocket.server.ServerEndpoint;
import java.io.IOException;
import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;


@ServerEndpoint("/userservice/{email}")
public class UserServerEndpoint {
    private Session session;
    private static Set<UserServerEndpoint> chatEndpoints= new CopyOnWriteArraySet<>();
    private static HashMap<String, String> users = new HashMap<>();

    @OnOpen
    public void onCreateSession(Session session, @PathParam("email") String username){
        chatEndpoints.add(this);
        users.put(session.getId(), username);
        this.session = session;
    }

    @OnMessage
    public void onRequest(String message){
        String arr[] = message.split(" ");
        Gson gson = new Gson();
        //When you login or register server returns user to front end
        if(arr[0].equals("register")){
            User u = UserWebSocketServer.userService.registerUser(arr[1], arr[2], arr[3], arr[4]);
            String serializedUser = gson.toJson(u);
            MessageWrapper toSend = new MessageWrapper("register",serializedUser);
            String serializedMessage = gson.toJson(toSend);
            System.out.println(UserWebSocketServer.userService.userList);
            sendMessage(serializedMessage);
        }
        else if(arr[0].equals("login")){
            User u = UserWebSocketServer.userService.Login(arr[1], arr[2]);
            String serializedUser = gson.toJson(u);
            MessageWrapper toSend = new MessageWrapper("login",serializedUser);
            String serializedMessage = gson.toJson(toSend);
            sendMessage(serializedMessage);
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
