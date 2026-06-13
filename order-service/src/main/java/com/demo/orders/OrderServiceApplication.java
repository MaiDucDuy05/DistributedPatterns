package com.demo.orders;

import org.springframework.amqp.core.TopicExchange;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.Optional;

@SpringBootApplication
@RestController
@RequestMapping("/api/orders")
public class OrderServiceApplication {

    @Autowired
    private RabbitTemplate rabbitTemplate;

    @Autowired
    private OrderRepository orderRepository;

    public static void main(String[] args) {
        SpringApplication.run(OrderServiceApplication.class, args);
    }

    @Bean
    public TopicExchange sagaExchange() {
        return new TopicExchange("saga_exchange");
    }

    @PostMapping
    public Map<String, Object> createOrder(@RequestBody Map<String, Object> payload) {
        String orderId = UUID.randomUUID().toString();
        String product = payload.getOrDefault("product", "unknown").toString();

        // 1. Lưu Order với trạng thái PENDING_INVENTORY
        Order order = new Order();
        order.setId(orderId);
        order.setProduct(product);
        order.setStatus("PENDING_INVENTORY");
        order.setCreatedAt(LocalDateTime.now());
        orderRepository.save(order);
        System.out.println("[Saga-Start] Nhận yêu cầu tạo đơn hàng: " + orderId);

        // 2. Publish Event order.created
        Map<String, String> eventMessage = new HashMap<>();
        eventMessage.put("orderId", orderId);
        eventMessage.put("product", product);
        
        rabbitTemplate.convertAndSend("saga_exchange", "order.created", eventMessage);
        System.out.println("[Saga-Start] Đã bắn Event 'order.created' lên RabbitMQ");

        // Gửi Message vào RabbitMQ (Các service khác có thể lắng nghe)
        String message = "Đã tạo đơn hàng mới: " + product;
        rabbitTemplate.convertAndSend("order_events", "", message);

        Map<String, Object> response = new HashMap<>();
        response.put("service", "Order Service (Java Spring Boot)");
        response.put("status", "success");
        response.put("orderId", orderId);
        response.put("order_status", "PENDING_INVENTORY");
        response.put("message", "Đã lưu đơn hàng vào DB và gửi tín hiệu trừ kho (Event-Driven)!");
        return response;
    }

    @GetMapping("/{id}")
    public Map<String, Object> getOrder(@PathVariable String id) {
        Optional<Order> orderOpt = orderRepository.findById(id);
        Map<String, Object> response = new HashMap<>();
        if (orderOpt.isPresent()) {
            response.put("order", orderOpt.get());
        } else {
            response.put("error", "Not found");
        }
        return response;
    }

    // Lắng nghe sự kiện trả về từ Inventory Service
    @RabbitListener(queues = "order_saga_queue")
    public void handleInventoryEvents(Map<String, String> event) {
        String eventType = event.get("type"); // inventory.deducted hoặc inventory.failed
        String orderId = event.get("orderId");
        
        Optional<Order> orderOpt = orderRepository.findById(orderId);
        if (orderOpt.isEmpty()) return;
        Order order = orderOpt.get();

        System.out.println("\n-------------------------------------");
        System.out.println("[Saga-Callback] Nhận Event từ Inventory: " + eventType + " cho Order: " + orderId);

        if ("inventory.deducted".equals(eventType)) {
            order.setStatus("CONFIRMED");
            System.out.println("[Saga-End] Cập nhật đơn hàng thành công (CONFIRMED)!");
        } else if ("inventory.failed".equals(eventType)) {
            // Xử lý giao dịch bù trừ (Compensating Transaction)
            order.setStatus("CANCELLED_NO_INVENTORY");
            System.out.println("\u001B[31m[Saga-Compensating] Kho báo hết hàng. Hủy đơn hàng và hoàn tiền (CANCELLED)!\u001B[0m");
        }

        orderRepository.save(order);
        System.out.println("-------------------------------------\n");
    }
}
