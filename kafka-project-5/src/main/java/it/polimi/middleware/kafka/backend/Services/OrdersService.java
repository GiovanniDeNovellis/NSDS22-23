package it.polimi.middleware.kafka.backend.Services;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
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

import java.lang.reflect.Type;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.*;

public class OrdersService implements Runnable{
    private Map<String, Integer> itemAvailability = new HashMap<>(); //ItemName - Quantity
    private List<Order> orderList = new ArrayList<>();
    private final String orderTopic = "orderCreation";
    private final String itemsTopic = "itemsCheckpoint";
    private final String serverAddr = "localhost:9092";
    private static List<String> topicsListened = new ArrayList<>();
    private static final boolean autoCommit = true;
    private static final int autoCommitIntervalMs = 15000;
    private static final String offsetResetStrategy = "latest";
    private int num_orders;

    public OrdersService() {
        recoverOrdersState();
        recoverItemsState();
    }

    //Reserved for Admin User type
    public Map<String, Integer> getItemAvailability(){
        return this.itemAvailability;
    }

    public void addItem(String type, int quantity){
        if(!itemAvailability.containsKey(type))
            itemAvailability.put(type,quantity);
        else{
            itemAvailability.replace(type,quantity);
        }
        publishNewItemState(itemAvailability);
        System.out.println("items updated");
    }

    //Called by the front end
    public String placeOrder(Map<String, Integer> orderedItems, String email, String address){
        if(!checkAvailability(orderedItems)){
            return "Unavailable Items";
        }
        updateItemList(orderedItems);
        Order order = new Order(email, address, orderedItems, "Created", num_orders);
        num_orders++;
        notifyNewOrder(order);
        //orderList.add(order);
        return "Successful order";
    }

    public List<Order> checkMyOrders(String email){
        System.out.println("Input email: " +  email);
        List<Order> MyOrders = new ArrayList<>();
        for(Order o: orderList){
            System.out.println("Order email: " + o.getCustomerEmail());
            if(email.equals(o.getCustomerEmail())){
                MyOrders.add(o);
            }
        }
        return MyOrders;
    }

    private boolean checkAvailability(Map<String, Integer> orderedItems){
        for(String key: orderedItems.keySet()){
            if(!itemAvailability.containsKey(key) || orderedItems.get(key)> itemAvailability.get(key))
                return false;
        }
        return true;
    }

    private void updateItemList(Map<String, Integer> orderedItems){
        for(String key: orderedItems.keySet()){
            int newQuantity = itemAvailability.get(key) - orderedItems.get(key);
            itemAvailability.replace(key, newQuantity);
        }
        publishNewItemState(itemAvailability);
    }

    private void notifyNewOrder(Order order){
        Gson gson = new Gson();
        final Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, serverAddr);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        String key = order.getCustomerEmail();
        String serializedOrder = gson.toJson(order);
        System.out.println("Sending: " + serializedOrder);
        ProducerRecord<String, String> record = new ProducerRecord<>(orderTopic, key, serializedOrder);
        producer.send(record);
    }

    private void publishNewItemState(Map<String, Integer> itemAvailability){
        Gson gson = new Gson();
        final Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, serverAddr);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        Random r = new Random();
        String key = "Key" + r.nextInt(1000);
        String serializedOItemState = gson.toJson(itemAvailability);
        ProducerRecord<String, String> record =
                new ProducerRecord<>(itemsTopic,null, System.currentTimeMillis(), key, serializedOItemState);
        System.out.println(record.timestamp());
        System.out.println("Notified state: " + serializedOItemState);
        producer.send(record);
    }

    private void handleOrderUpdate(Order updatedOrder){
        Order oldOrderVersion = null;
        for(Order order: orderList) {
            if (order.getId() == updatedOrder.getId())
                oldOrderVersion=order;
        }
        orderList.remove(oldOrderVersion);
        orderList.add(updatedOrder);
    }

    private void handleOrderRecovery(Order recoveredOrder){
        Order oldVersion=null;
        for(Order order: orderList){
            if(order.getId()==recoveredOrder.getId())
                oldVersion=order;
        }
        if(oldVersion==null){
            orderList.add(recoveredOrder);
            num_orders++;
        }
        else{
            if(oldVersion.getLogicalVersion()< recoveredOrder.getLogicalVersion()){
                orderList.remove(oldVersion);
                orderList.add(recoveredOrder);
            }
        }
    }

    private void recoverOrdersState(){
        Gson gson = new Gson();
        System.out.println("Orders recovery started");
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, serverAddr);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "groupOrdRecovery");
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
        for(Order u: orderList){
            System.out.println(u);
        }
        //This is needed otherwise after one recovery in the next crash it won't reread the messages
        // (It will count the messages as already delivered to this consumer group)
        consumer.unsubscribe();
    }

    private void recoverItemsState(){
        Gson gson = new Gson();
        System.out.println("Items state recovery started");
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, serverAddr);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "itemsConf");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, String.valueOf(autoCommit));
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, String.valueOf(autoCommitIntervalMs));
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singleton("itemsCheckpoint"));
        final ConsumerRecords<String, String> records = consumer.poll(Duration.of(5, ChronoUnit.SECONDS));
        consumer.seekToBeginning(records.partitions());
        if(records.isEmpty())
            System.out.println("No state of items found");
        long lastTimestamp=0;
        for (final ConsumerRecord<String, String> record : records) {
            System.out.println(record.topic());
            System.out.println(record.value());
            Type mapType = new TypeToken<Map<String, Integer>>(){}.getType();
            Map<String, Integer> recoveredState = gson.fromJson(record.value(), mapType);
            if(record.timestamp()>lastTimestamp) {
                itemAvailability = recoveredState;
                lastTimestamp= record.timestamp();
            }

        }
        System.out.println("Items recovery recovery ended");
        System.out.println(itemAvailability.toString());
        //This is needed otherwise after one recovery in the next crash it won't reread the messages
        // (It will count the messages as already delivered to this consumer group)
        consumer.unsubscribe();
    }

    //Consumer thread for the orders service that will listen for updates of the order status
    //by the shipping service
    @Override
    public void run() {
        Gson gson = new Gson();
        topicsListened.add("orderCreation");
        System.out.println("Order consumer started");
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, serverAddr);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "groupOrdListener");
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
                Order updatedOrder = gson.fromJson(record.value(), Order.class);
                handleOrderUpdate(updatedOrder);
            }
        }
    }
}
