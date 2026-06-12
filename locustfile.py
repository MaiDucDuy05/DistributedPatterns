# pyrefly: ignore [missing-import]
from locust import HttpUser, task, between

class APIUser(HttpUser):
    wait_time = between(0.1, 0.5)

    @task
    def hit_api(self):
        self.client.get("/api")
