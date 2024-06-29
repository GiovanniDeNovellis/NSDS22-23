package it.polimi.middleware.kafka.backend.Services;

import org.glassfish.tyrus.server.Server;

import java.io.BufferedReader;
import java.io.InputStreamReader;

public class OrderWebSocketServer {
    static OrdersService ordersService = new OrdersService();
    public static void main(String[] args) {
        runServer();
    }

    public static void runServer() {
        Thread t = new Thread(ordersService);
        t.start();
        Server server = new Server("localhost", 8026, "/websockets", OrderServerEndpoint.class);
        try {
            server.start();
            BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
            System.out.println("Please press a key to stop the server.");
            reader.readLine();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            server.stop();
        }
    }
}
