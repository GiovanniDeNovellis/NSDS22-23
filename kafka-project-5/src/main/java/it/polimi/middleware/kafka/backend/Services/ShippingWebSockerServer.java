package it.polimi.middleware.kafka.backend.Services;

import org.glassfish.tyrus.server.Server;

import java.io.BufferedReader;
import java.io.InputStreamReader;

public class ShippingWebSockerServer {
    static ShippingService shippingService = new ShippingService();

    public static void main(String[] args) {runServer();}

    public static void runServer() {
        Thread t= new Thread(shippingService);
        t.start();
        Server server = new Server("localhost", 8027, "/websockets", ShippingServerEndpoint.class);
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
