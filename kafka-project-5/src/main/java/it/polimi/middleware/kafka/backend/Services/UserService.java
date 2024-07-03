package it.polimi.middleware.kafka.backend.Services;

import com.google.gson.Gson;
import it.polimi.middleware.kafka.backend.Entities.User;
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
import java.util.List;

public class UserService {
    public List<User> userList = new ArrayList<>();

    private final String defaultTopic = "userCreation";

    private final String serverAddr = "localhost:9092";

    private static final boolean autoCommit = true;
    private static final int autoCommitIntervalMs = 15000;
    private static final String offsetResetStrategy = "earliest";
    private static final String defaultGroupId = "groupUser";

    //Recover the list of users when the service starts
    public UserService() {
        System.out.println("Constructor");
        recoverUsersState();
    }

    //Adding a new users to the system and publishing a message for future recoveries
    public User registerUser(String email, String password, String type, String address){
        for(User u: userList){
            if(u.getEmail().equals(email)) {
                System.out.println("User already exists."); //Email already present in the system, can't register
                return null;
            }
        }
        User user = new User(email, password, type);
        if(type.equals("Customer")){
            user.setAddress(address);
        }
        userList.add(user);
        notifyRegistration(user);
        return user;
    }

    //Handle loginr equests by an user
    public User Login(String email, String password){
        for(User u: userList){
            if(u.getEmail().equals(email) && u.getPassword().equals(password)){
                return u;
            }
        }
        return null;
    }

    //Publish a new user message
    private void notifyRegistration(User user){
        //Library to serialize the object
        Gson gson = new Gson();
        final Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, serverAddr);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        String key = user.getEmail();
        String serializedUser = gson.toJson(user);
        System.out.println(serializedUser);
        ProducerRecord<String, String> record = new ProducerRecord<>(defaultTopic, key, serializedUser);
        producer.send(record);
    }

    //Recover the list of users when the service starts
    private void recoverUsersState(){
        Gson gson = new Gson();
        System.out.println("Users recovery started");
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, serverAddr);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, defaultGroupId);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, String.valueOf(autoCommit));
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, String.valueOf(autoCommitIntervalMs));
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offsetResetStrategy);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singleton(defaultTopic));
        final ConsumerRecords<String, String> records = consumer.poll(Duration.of(5, ChronoUnit.SECONDS));
        consumer.seekToBeginning(records.partitions());
        if(records.isEmpty())
            System.out.println("No users found");
        for (final ConsumerRecord<String, String> record : records) {
            System.out.println(record.topic());
            System.out.println(record.value());
            User user = gson.fromJson(record.value(), User.class);
            userList.add(user);
        }
        System.out.println("User recovery ended");
        for(User u: userList){
            System.out.println(u);
        }
        //This is needed otherwise after one recovery in the next crash it won't reread the messages
        // (It will count the messages as already delivered to this consumer group)
        consumer.unsubscribe();
    }
}

