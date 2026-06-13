package com.demo.orders;

import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import io.github.resilience4j.retry.annotation.Retry;
import java.util.Map;

@Service
public class InventoryClient {

    private RestTemplate restTemplate = new RestTemplate();

    @Retry(name = "inventoryRetry", fallbackMethod = "inventoryFallback")
    @CircuitBreaker(name = "inventoryCircuitBreaker", fallbackMethod = "inventoryFallback")
    public String checkInventorySync() {
        System.out.println("--- Đang gọi đồng bộ sang Inventory Service để kiểm tra kho... ---");
        String url = "http://inventory-service:8001/api/inventory/check";
        Map<String, String> response = restTemplate.getForObject(url, Map.class);
        return response.get("message");
    }

    public String inventoryFallback(Exception e) {
        System.out.println("\u001B[31m[Fallback] Kích hoạt Fallback do lỗi Inventory: " + e.getMessage() + "\u001B[0m");
        return "Bỏ qua kiểm tra kho. Chấp nhận rủi ro cho phép đặt hàng (Fallback)";
    }
}
