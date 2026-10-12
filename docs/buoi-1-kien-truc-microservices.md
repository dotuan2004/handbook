# Buổi 1 — Kiến trúc Microservices & Giao tiếp giữa các Service

## 1. Kiến trúc Per Service – Per Database

- Mỗi service sở hữu một database logic riêng.
- Nhiều database có thể cùng nằm trên một DB server vật lý hoặc một DB instance để tiết kiệm chi phí.
- Các service vẫn phải truy cập dữ liệu thông qua API/message, **không truy cập trực tiếp** database của service khác.

## 2. Các loại giao tiếp giữa Service

### REST vs gRPC

| Tiêu chí | REST | gRPC |
| -------- | ---- | ---- |
| Giao thức | HTTP/JSON | Protocol Buffers, HTTP/2 |
| Ưu điểm | Dễ tích hợp | Hiệu năng cao, hỗ trợ streaming, contract rõ ràng |
| Phù hợp | API FE ↔ BE, hoặc giữa các service | Giao tiếp nội bộ cần hiệu năng và contract rõ ràng |

### Lựa chọn giao tiếp giữa các Service

#### 1. Response time (Thời gian phản hồi)

- **Đồng bộ (REST/gRPC):** cần kết quả ngay để trả về cho client hoặc quyết định bước tiếp theo.
- **Bất đồng bộ (Kafka/RabbitMQ):** chỉ cần xác nhận đã tiếp nhận yêu cầu, xử lý nghiệp vụ có thể diễn ra sau.

#### 2. Nghiệp vụ

- **Đồng bộ:** Service A cần kết quả từ Service B để quyết định có tiếp tục xử lý hay không.
- **Bất đồng bộ:** A phát sinh sự kiện, B nhận sự kiện và tự xử lý nghiệp vụ của mình.

Ví dụ:
- Kiểm tra quyền truy cập trước khi trả dữ liệu → **đồng bộ**.
- Phát event `UserRegistered` để gửi email chào mừng → **bất đồng bộ**.

#### 3. Độ dài luồng logic

- **Đồng bộ:** phù hợp với luồng ngắn, cần kết quả tức thời.
- **Bất đồng bộ:** phù hợp với luồng dài, gồm nhiều bước hoặc nhiều service có thể xử lý độc lập.

Ví dụ luồng đặt hàng:

```
Order → Payment → Inventory → Notification
```

Nếu tất cả đều gọi đồng bộ, thời gian phản hồi tăng theo tổng latency của các service; lỗi hoặc timeout ở một service có thể ảnh hưởng toàn bộ chuỗi.

**Giải pháp:**
- Dùng đồng bộ cho những bước cần quyết định ngay.
- Dùng bất đồng bộ cho các bước xử lý tiếp theo.
- Dùng **Saga** để quản lý trạng thái nghiệp vụ và compensation khi một bước thất bại.

#### 4. Khả năng chịu tải

Giả sử Service A gửi 1.000 request/s nhưng Service B chỉ xử lý được 300 request/s.

- **Đồng bộ:** B dễ quá tải, request chờ lâu, timeout và có thể kéo theo lỗi ở A. Cần timeout, circuit breaker, giới hạn concurrency và backpressure.
- **Bất đồng bộ:** Kafka/RabbitMQ có thể hấp thụ message tạm thời để B xử lý theo năng lực của mình.

> Lưu ý: nếu tốc độ gửi liên tục lớn hơn tốc độ xử lý, backlog sẽ tăng. Cần theo dõi **consumer lag**, throughput và scale consumer phù hợp.

#### 5. Khả năng retry và phục hồi khi lỗi

- **Đồng bộ:** có thể retry request khi gặp lỗi tạm thời. Nếu luồng đã thực hiện một số bước thành công, không nên mặc định chạy lại từ đầu vì có thể tạo giao dịch trùng hoặc lặp side effect.
- **Bất đồng bộ:** có thể retry xử lý message thất bại, sử dụng **retry queue** hoặc **DLQ** (Dead Letter Queue) để lưu các message chưa xử lý thành công và điều tra sau.

Ví dụ:

```
OrderCreated → Reserve Inventory → Payment → Notification
```

Nếu gửi Notification thất bại, hệ thống có thể retry riêng bước gửi thông báo thay vì tạo lại đơn hàng và giữ hàng lần nữa.
