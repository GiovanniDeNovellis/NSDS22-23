package it.polimi.middleware.kafka.frontend;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Scanner;

import it.polimi.middleware.kafka.backend.Entities.User;
import org.junit.Before;

import javax.websocket.*;


public class Frontend {

    private WebSocketContainer container;
    private ClientEndpoint endpointuser,endpointorder, endpointshipping;

    @Before
    public  void onInit(){
        this.container = ContainerProvider.getWebSocketContainer();
        this.endpointuser = new ClientEndpoint();
        this.endpointorder = new ClientEndpoint();
        this.endpointshipping = new ClientEndpoint();
    }

    public void connectUserServer( String email) throws URISyntaxException, DeploymentException, IOException {
        this.container.connectToServer(this.endpointuser,new URI("ws://localhost:8025/websockets/userservice/"+email));

    }

    public void sendUserMessage(String message) throws IOException {
        this.endpointuser.sendMessage(message);

    }

    public void connectOrderServer( String email) throws URISyntaxException, DeploymentException, IOException {
        this.container.connectToServer(this.endpointorder,new URI("ws://localhost:8026/websockets/orderservice/"+email));

    }

    public void sendOrderMessage(String message) throws IOException {
        this.endpointorder.sendMessage(message);

    }

    public void connectShippingServer() throws URISyntaxException, DeploymentException, IOException {
        this.container.connectToServer(this.endpointshipping,new URI("ws://localhost:8027/websockets/shippingservice"));
    }

    public void sendShippingMessage(String message) throws IOException{
        this.endpointshipping.sendMessage(message);

    }

    public boolean isLogged(){
        return endpointuser.myUser!=null;
    }

    public User getUser(){
        return endpointuser.myUser;
    }

    public static void main(String[] args) throws DeploymentException, URISyntaxException, IOException, InterruptedException {
        Frontend newSession = new Frontend();
        newSession.onInit();

        Scanner sc = new Scanner(System.in);
        String command = "";
        String[] args1;

        // HANDLING LOGIN / REGISTRATION
        while (!newSession.isLogged()) {
            System.out.println("Welcome! You can login or register by typing login or register.");
            command = sc.nextLine();
            args1 = command.split(" ");
            if (args1[0].equals("register")) {
                System.out.println("Please input your email password customer_type and address");
                System.out.println("(valid customer types are: Customer, Delivery or Admin).");
                command = sc.nextLine();
                String[] args2 = command.split(" ");
                if(args2.length == 4) {
                    while (!(args2[2].equals("Customer") || args2[2].equals("Delivery") || args2[2].equals("Admin"))) {
                        System.out.println("Invalid customer_type, please type customer_type again.");
                        args2[2] = sc.nextLine();
                    }
                    command = "register "+args2[0]+" "+args2[1]+" "+args2[2]+" "+args2[3];
                    newSession.connectUserServer(args2[0]);
                    newSession.sendUserMessage(command);
                }
                else System.out.println("Wrong number of arguments.");
            } else if (args1[0].equals("login")) {
                System.out.println("Please input your email and password.");
                command = sc.nextLine();
                String[] args2 = command.split(" ");
                if(args2.length == 2) {
                    newSession.connectUserServer(args2[0]);
                    newSession.sendUserMessage("login " + command);
                }
                else System.out.println("Wrong number of arguments.");
            }
            else
                System.out.println("Invalid command.");
            Thread.sleep(4000);
        }

        //CONNECTING TO THE REQUIRED SERVICES
        System.out.println("Welcome " + newSession.getUser().getEmail() + "!");
        if(newSession.getUser().getType().equals("Delivery")) {
            newSession.connectShippingServer();
            System.out.println("Connected to shipping service.");
        }
        else{
            newSession.connectOrderServer(newSession.getUser().getEmail());
            System.out.println("Connected to order service.");
        }

        //HANDLING "BUSINESS" COMMANDS
        while (!(command.equals("exit"))) {
            //CUSTOMER COMMANDS
            if(newSession.getUser().getType().equals("Customer")){
                System.out.println("You can do the following:");
                System.out.println("1. Place an order by typing \"order\"");
                System.out.println("2. Show order history by typing \"history\"");
                System.out.println("3. Show available items by typing \"items\"");
                command = sc.nextLine();
                args1 = command.split(" ");
                switch (args1[0]) {
                    case "order":
                        System.out.println("please type the items you want in the form \"apple\" 1 \"chocolate\" 5 ... and the press enter");
                        command = sc.nextLine();
                        newSession.sendOrderMessage("order " + command + " " + newSession.getUser().getAddress());
                        Thread.sleep(3000);
                        break;
                    case "history":
                        newSession.sendOrderMessage("history");
                        Thread.sleep(3000);
                        break;
                    case "items":
                        newSession.sendOrderMessage("items");
                        Thread.sleep(3000);
                        break;
                    default:
                        System.out.println("Please type a valid command");
                        break;
                }
            }
            //DELIVERY COMMANDS
            else if(newSession.getUser().getType().equals("Delivery")) {
                System.out.println("You can do the following:");
                System.out.println("1. Update order status by typing \"update_order\"");
                System.out.println("2. See pending deliveries by typing \"show_deliveries\"");
                command = sc.nextLine();
                args1 = command.split(" ");
                if (args1[0].equals("update_order")){
                    System.out.println("Please write the id of the order to mark as delivered ");
                    command=sc.nextLine();
                    newSession.sendShippingMessage("update_order " + command);
                }
                else if(args1[0].equals("show_deliveries")){
                    newSession.sendShippingMessage("show_deliveries");
                }
            }
            //ADMIN COMMANDS
            else if(newSession.getUser().getType().equals("Admin")) {
                System.out.println("You can do the following:");
                System.out.println("1. Update available items by typing  \"update_items\"");
                System.out.println("2. Show current availability by typing  \"items\"");
                command = sc.nextLine();
                args1 = command.split(" ");
                if (args1[0].equals("update_items")){
                    System.out.println("please enter the type_of_product and quantity to add");
                    command = sc.nextLine();
                    String[] args2 = command.split(" ");
                    if(args2.length == 2){
                        newSession.sendOrderMessage("update_items "+command);
                        Thread.sleep(3000);
                    }
                    else System.out.println("wrong number of arguments!");
                }
                else if (args1[0].equals("items")) {
                    newSession.sendOrderMessage("items");
                    Thread.sleep(3000);
                }
            }
            else{
                System.err.println("Invalid user type");
                return;
            }
        }
    }
}
