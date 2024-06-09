package com.example;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

public class CSV {

    public static void main(String[] args) {
        Random rand = new Random();
        try (FileWriter writer = new FileWriter("data/orders.csv")) {
            writer.append("order_id,customer_id,order_date,amount\n");
            for (int i = 1; i <= 20000; i++) {
                int customerId = rand.nextInt(40) + 1;
                String orderDate = "2023-05-" + String.format("%02d", (rand.nextInt(30) + 1)); 
                double amount = rand.nextDouble() * 100;
                writer.append(i + "," + customerId + "," + orderDate + "," + String.format("%.2f", amount) + "\n");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
