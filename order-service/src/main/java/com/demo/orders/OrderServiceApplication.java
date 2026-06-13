package com.demo.orders;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.web.bind.annotation.*;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.HashMap;
import java.util.Map;
import org.springframework.amqp.core.FanoutExchange;
import org.springframework.context.annotation.Bean;
import org.springframework.web.client.RestTemplate;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import io.github.resilience4j.retry.annotation.Retry;

@SpringBootApplication
@RestController
@RequestMapping("/api/orders")
public class OrderServiceApplication {

    @Autowired
    private RabbitTemplate rabbitTemplate;

    @Bean
    public FanoutExchange orderEventsExchange() {
        return new FanoutExchange("order_events");
    }

    public static void main(String[] args) {
        SpringApplication.run(OrderServiceApplication.class, args);
    }

    @Autowired
    private InventoryClient inventoryClient;

    @PostMapping
    public Map<String, Object> createOrder(@RequestBody Map<String, Object> payload) {
        System.out.println("Nhận yêu cầu tạo đơn hàng: " + payload);
        
        // Gọi đồng bộ (Sẽ nhảy Circuit Breaker nếu Inventory lỗi hoặc chậm)
        String inventoryStatus = inventoryClient.checkInventorySync();
        System.out.println("Trạng thái kho: " + inventoryStatus);

        // Gửi Message vào RabbitMQ (Các service khác có thể lắng nghe)
        String message = "Đã tạo đơn hàng mới: " + payload.getOrDefault("product", "unknown");
        
        // Lưu ý: Đổi từ việc gửi vào 1 Queue sang gửi vào Fanout Exchange (Broadcast)
        rabbitTemplate.convertAndSend("order_events", "", message);

        Map<String, Object> response = new HashMap<>();
        response.put("service", "Order Service (Java Spring Boot)");
        response.put("status", "success");
        response.put("inventory_check", inventoryStatus);
        response.put("message", "Đã lưu đơn hàng và gửi tin nhắn vào RabbitMQ!");
        response.put("data_received", payload);
        return response;
    }

    @GetMapping
    public Map<String, String> getInfo() {
        Map<String, String> info = new HashMap<>();
        info.put("service", "Order Service (Java Spring Boot)");
        info.put("message", "Hãy gửi POST request chứa JSON vào đây để tạo đơn hàng");
        return info;
    }
}
