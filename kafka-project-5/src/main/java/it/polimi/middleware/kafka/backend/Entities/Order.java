package it.polimi.middleware.kafka.backend.Entities;

import java.util.HashMap;
import java.util.Map;

public class Order {
    private int id;
    private String customerEmail;
    private String customerAddress;
    private String status;
    private Map<String, Integer> boughtItems = new HashMap<>(); //Item - Quantity
    private int logicalVersion;

    public Order(String customerEmail, String customerAddress, Map<String, Integer> boughtItems,
                 String status, int id) {
        this.customerEmail = customerEmail;
        this.customerAddress = customerAddress;
        this.boughtItems = boughtItems;
        this.status=status;
        this.id=id;
        logicalVersion=0;
    }

    public String getCustomerEmail() {
        return customerEmail;
    }

    public void setCustomerEmail(String customerEmail) {
        this.customerEmail = customerEmail;
    }

    public String getCustomerAddress() {
        return customerAddress;
    }

    public void setCustomerAddress(String customerAddress) {
        this.customerAddress = customerAddress;
    }

    public Map<String, Integer> getBoughtItems() {
        return boughtItems;
    }

    public void setBoughtItems(Map<String, Integer> boughtItems) {
        this.boughtItems = boughtItems;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public int getId() {
        return id;
    }

    public int getLogicalVersion() {
        return logicalVersion;
    }

    public void increaseLogicalVersion(){
        logicalVersion++;
    }

    @Override
    public String toString() {
        return "Order{" +
                "id=" + id +
                ", customerEmail='" + customerEmail + '\'' +
                ", customerAddress='" + customerAddress + '\'' +
                ", status='" + status + '\'' +
                ", boughtItems=" + boughtItems +
                '}';
    }
}
