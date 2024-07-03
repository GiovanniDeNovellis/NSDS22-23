package it.polimi.middleware.kafka.backend.Services;

import com.google.gson.Gson;
import it.polimi.middleware.kafka.backend.Entities.Order;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.*;

public class ShippingService implements Runnable{
    private final String orderTopic = "orderCreation";

    private static List<Order> ongoingOrders = new ArrayList<>();

    private static List<String> topicsListened = new ArrayList<>();
    private static final String serverAddr = "localhost:9092";
    private static final boolean autoCommit = true;
    private static final int autoCommitIntervalMs = 15000;
    private static final String offsetResetStrategy = "latest";
    private static final String defaultGroupId = "groupShip";

    public ShippingService() {
        recoverOrdersState();
    }

    //Called by frontend
    public List<Order> getOngoingDeliveries(){
        return ongoingOrders;
    }

    //Update the status of the order when the delivery employee reports the delivery.
    public String updateOrder(String status, int orderId){
        Order orderToUpdate = findOrder(orderId);
        if(orderToUpdate==null)
            return "Order Not Found";
        else{
            if(status.equals("Delivered"))
                ongoingOrders.remove(orderToUpdate);
        }
        orderToUpdate.setStatus(status);
        notifyOrderUpdate(orderToUpdate);
        return "Successful Update";
    }

    //Retrieve the needed order
    private Order findOrder(int id){
        for(Order order: ongoingOrders){
            if(order.getId()==id)
                return order;
        }
        return null;
    }

    //Publish a message for the orders service when an order's status is updated.
    private void notifyOrderUpdate(Order updatedOrder){
        Gson gson = new Gson();
        final Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, serverAddr);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        updatedOrder.increaseLogicalVersion();
        String key = updatedOrder.getCustomerEmail();
        String serializedOrder = gson.toJson(updatedOrder);
        ProducerRecord<String, String> record = new ProducerRecord<>(orderTopic, key, serializedOrder);
        producer.send(record);
    }

    //Save a new order when a message is received
    private static void handleNewOrder(String message){
        Gson gson = new Gson();
        Order order = gson.fromJson(message, Order.class);
        if(order.getStatus().equals("Delivered"))
            return;
        ongoingOrders.add(order);
    }

    //Recovering the pending orders in case the service crashes
    private void handleOrderRecovery(Order recoveredOrder){
        Order oldVersion=null;
        for(Order order: ongoingOrders){
            if(order.getId()==recoveredOrder.getId())
                oldVersion=order;
        }
        if(oldVersion==null){
            ongoingOrders.add(recoveredOrder);
        }
        else{
            if(oldVersion.getLogicalVersion()< recoveredOrder.getLogicalVersion()){
                ongoingOrders.remove(oldVersion);
                ongoingOrders.add(recoveredOrder);
            }
        }
    }

    //Only pending orders are needed by the shipping service
    private void removeDeliveredOrders(){
        for(int i=0; i<ongoingOrders.size(); i++){
            Order ord= ongoingOrders.get(i);
            if(ord.getStatus().equals("Delivered"))
                ongoingOrders.remove(ord);
        }
    }

    //Recovering the state of orders after a crash
    private void recoverOrdersState(){
        Gson gson = new Gson();
        System.out.println("Orders recovery started");
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, serverAddr);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "shipOrdRecovery");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, String.valueOf(autoCommit));
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, String.valueOf(autoCommitIntervalMs));
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singleton("orderCreation"));
        final ConsumerRecords<String, String> records = consumer.poll(Duration.of(5, ChronoUnit.SECONDS));
        consumer.seekToBeginning(records.partitions());
        if(records.isEmpty())
            System.out.println("No orders found");
        for (final ConsumerRecord<String, String> record : records) {
            System.out.println(record.topic());
            System.out.println(record.value());
            Order order = gson.fromJson(record.value(), Order.class);
            handleOrderRecovery(order);
        }
        System.out.println("Order recovery ended");
        removeDeliveredOrders();
        for(Order u: ongoingOrders){
            System.out.println(u);
        }
        consumer.unsubscribe();
    }

    //Running consumer that listens for a new order placed by the Orders Service
    @Override
    public void run() {
        topicsListened.add("orderCreation");
        System.out.println("Shipping consumer started");
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, serverAddr);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, defaultGroupId);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, String.valueOf(autoCommit));
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, String.valueOf(autoCommitIntervalMs));
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offsetResetStrategy);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(topicsListened);
        while (true) {
            final ConsumerRecords<String, String> records = consumer.poll(Duration.of(5, ChronoUnit.SECONDS));
            for (final ConsumerRecord<String, String> record : records) {
                System.out.println(record.topic());
                System.out.println(record.value());
                if(record.topic().equals("orderCreation"))
                    handleNewOrder(record.value());
            }
        }
    }
}
