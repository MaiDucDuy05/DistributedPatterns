package com.demo.orders;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.web.bind.annotation.*;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.HashMap;
import java.util.Map;
import org.springframework.amqp.core.Queue;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
@RestController
@RequestMapping("/api/orders")
public class OrderServiceApplication {

    @Autowired
    private RabbitTemplate rabbitTemplate;

    @Bean
    public Queue orderQueue() {
        return new Queue("order_queue", true);
    }

    public static void main(String[] args) {
        SpringApplication.run(OrderServiceApplication.class, args);
    }

    @PostMapping
    public Map<String, Object> createOrder(@RequestBody Map<String, Object> payload) {
        System.out.println("Nhận yêu cầu tạo đơn hàng: " + payload);
        
        // Gửi Message vào RabbitMQ (Các service khác có thể lắng nghe)
        String message = "Đã tạo đơn hàng mới: " + payload.getOrDefault("product", "unknown");
        
        // Lưu ý: Trong thực tế ta cần cấu hình Exchange/Queue chi tiết hơn, 
        // ở đây dùng gửi trực tiếp vào queue mặc định cho đơn giản.
        rabbitTemplate.convertAndSend("order_queue", message);

        Map<String, Object> response = new HashMap<>();
        response.put("service", "Order Service (Java Spring Boot)");
        response.put("status", "success");
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
