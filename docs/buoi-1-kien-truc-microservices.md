# Buổi 1 — Tổng hợp kiến thức Microservices

## 1. Monolith vs Microservices

### 1.1. Định nghĩa

**Monolith:** toàn bộ ứng dụng (UI, business logic, truy cập dữ liệu) nằm trong một codebase, build thành một artifact, deploy thành một process và thường dùng chung một database.

**Microservices:** chia ứng dụng thành nhiều service nhỏ, độc lập:
- Mỗi service phụ trách một nghiệp vụ (bounded context).
- Mỗi service có codebase riêng, deploy riêng, thường có database riêng (database per service).
- Các service giao tiếp qua mạng: REST/HTTP, gRPC (đồng bộ), hoặc message broker như Kafka/RabbitMQ (bất đồng bộ).

### 1.2. Vì sao chọn Microservices thay vì Monolith?

Monolith bắt đầu "đau" khi hệ thống lớn lên:

| Vấn đề của monolith | Microservices giải quyết thế nào |
| ------------------- | -------------------------------- |
| Code phình to, khó hiểu, sửa chỗ này vỡ chỗ kia | Mỗi service nhỏ, ranh giới rõ, dễ nắm |
| Nhiều team cùng sửa một repo, hay conflict, phải chờ nhau release | Mỗi team sở hữu một service, release độc lập |
| Chỉ một module chịu tải cao mà phải scale cả khối | Scale riêng service đó |
| Một module lỗi (memory leak, crash) kéo sập toàn hệ thống | Lỗi được cô lập trong từng service |
| Bị khoá vào một công nghệ duy nhất | Mỗi service chọn stack phù hợp (Java, Python, Node...) |
| Build, test, deploy cả khối rất chậm | Pipeline CI/CD riêng cho từng service, deploy nhanh và thường xuyên |

### 1.3. Ưu điểm của Microservices

- **Scale độc lập:** chỉ scale phần cần thiết, tiết kiệm tài nguyên.
- **Deploy độc lập:** release nhanh, rủi ro mỗi lần deploy nhỏ, rollback dễ.
- **Cô lập lỗi (fault isolation):** một service chết không nhất thiết kéo cả hệ thống chết, nếu có circuit breaker, retry, fallback.
- **Linh hoạt công nghệ (polyglot):** chọn ngôn ngữ và database phù hợp từng bài toán.
- **Team tự chủ:** team nhỏ, sở hữu trọn vòng đời service (Conway's Law), giảm phụ thuộc giữa các team.
- **Dễ bảo trì, dễ thay thế:** service nhỏ có thể viết lại mà không đụng phần khác.
- **Onboarding nhanh hơn với từng phần:** dev mới chỉ cần hiểu một service.

### 1.4. Nhược điểm của Microservices

- **Phức tạp của hệ thống phân tán:** độ trễ mạng, lỗi một phần (partial failure), timeout, retry, idempotency.
- **Dữ liệu nhất quán khó:** không có transaction ACID xuyên service. Phải dùng Saga, eventual consistency, Outbox pattern.
- **Vận hành nặng:** cần container/K8s, CI/CD, service discovery, API gateway, config tập trung.
- **Debug và test khó hơn:** lỗi trải qua nhiều service. Cần contract test và integration test giữa các service.
- **Chi phí hạ tầng và nhân lực cao hơn:** nhiều service, nhiều pipeline, cần DevOps.
- **Bảo mật:** nhiều điểm giao tiếp nội bộ hơn (auth giữa service, mTLS, token propagation).
- **Dễ chia sai ranh giới:** chia quá nhỏ dẫn đến "distributed monolith" — vừa có độ phức tạp của micro vừa bị ràng buộc chặt như mono.

## 2. Kiến trúc Per Service – Per Database

- Mỗi service sở hữu một database logic riêng.
- Nhiều database có thể cùng nằm trên một DB server vật lý hoặc một DB instance để tiết kiệm chi phí.
- Các service vẫn phải truy cập dữ liệu thông qua API/message, **không truy cập trực tiếp** database của service khác.

## 3. Các loại giao tiếp giữa Service

### REST vs gRPC

| Tiêu chí | REST | gRPC |
| -------- | ---- | ---- |
| Giao thức | HTTP/JSON | Protocol Buffers, HTTP/2 |
| Ưu điểm | Dễ tích hợp | Hiệu năng cao, hỗ trợ streaming, contract rõ ràng |
| Phù hợp | API FE ↔ BE, hoặc giữa các service | Giao tiếp nội bộ cần hiệu năng và contract rõ ràng |

### Đồng bộ vs Bất đồng bộ

**Đồng bộ (Synchronous)** — REST, gRPC
- Service A gọi B → chờ B trả response.
- B lỗi/timeout → A có thể bị ảnh hưởng.

**Bất đồng bộ (Asynchronous)** — Kafka, RabbitMQ
- A gửi message/event → không cần chờ B xử lý.
- Giúp giảm coupling, xử lý background, scale tốt.

### Lựa chọn giao tiếp giữa các Service

#### 3.1. Response time (Thời gian phản hồi)

- **Đồng bộ (REST/gRPC):** cần kết quả ngay để trả về cho client hoặc quyết định bước tiếp theo.
- **Bất đồng bộ (Kafka/RabbitMQ):** chỉ cần xác nhận đã tiếp nhận yêu cầu, xử lý nghiệp vụ có thể diễn ra sau.

#### 3.2. Nghiệp vụ

- **Đồng bộ:** Service A cần kết quả từ Service B để quyết định có tiếp tục xử lý hay không.
- **Bất đồng bộ:** A phát sinh sự kiện, B nhận sự kiện và tự xử lý nghiệp vụ của mình.

Ví dụ:
- Kiểm tra quyền truy cập trước khi trả dữ liệu → **đồng bộ**.
- Phát event `UserRegistered` để gửi email chào mừng → **bất đồng bộ**.

#### 3.3. Độ dài luồng logic

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

#### 3.4. Khả năng chịu tải

Giả sử Service A gửi 1.000 request/s nhưng Service B chỉ xử lý được 300 request/s.

- **Đồng bộ:** B dễ quá tải, request chờ lâu, timeout và có thể kéo theo lỗi ở A. Cần timeout, circuit breaker, giới hạn concurrency và backpressure.
- **Bất đồng bộ:** Kafka/RabbitMQ có thể hấp thụ message tạm thời để B xử lý theo năng lực của mình.

> Lưu ý: nếu tốc độ gửi liên tục lớn hơn tốc độ xử lý, backlog sẽ tăng. Cần theo dõi **consumer lag**, throughput và scale consumer phù hợp.

#### 3.5. Khả năng retry và phục hồi khi lỗi

- **Đồng bộ:** có thể retry request khi gặp lỗi tạm thời. Nếu luồng đã thực hiện một số bước thành công, không nên mặc định chạy lại từ đầu vì có thể tạo giao dịch trùng hoặc lặp side effect.
- **Bất đồng bộ:** có thể retry xử lý message thất bại, sử dụng **retry queue** hoặc **DLQ** (Dead Letter Queue) để lưu các message chưa xử lý thành công và điều tra sau.

Ví dụ:

```
OrderCreated → Reserve Inventory → Payment → Notification
```

Nếu gửi Notification thất bại, hệ thống có thể retry riêng bước gửi thông báo thay vì tạo lại đơn hàng và giữ hàng lần nữa.

## 4. Kafka

### 4.1. Khái niệm

| Khái niệm | Hiểu đơn giản |
| --------- | ------------- |
| Producer | Service gửi message |
| Consumer | Service đọc message |
| Topic | Nơi chứa các message cùng loại |
| Partition | Chia topic thành nhiều phần để scale/parallel |
| Offset | Vị trí của message trong partition |
| Broker | Một Kafka server |
| Cluster | Nhiều broker kết hợp thành một Kafka |
| Consumer Group | Nhóm consumer cùng xử lý một topic |

Ví dụ:

```
Producer
   ↓
Topic: order-created
   ↓
┌────────────┬────────────┬────────────┐
│ Partition 0│ Partition 1│ Partition 2│
└────────────┴────────────┴────────────┘
       ↓            ↓            ↓
   Consumer 1   Consumer 2   Consumer 3
```

### 4.2. Offset

Kafka lưu vị trí consumer đã xử lý thông qua:

```
Consumer Group + Partition + Committed Offset
```

Ví dụ:
```
Partition 0 → offset 100
Consumer restart → tiếp tục từ offset đã commit.
```

### 4.3. `auto.offset.reset`

Dùng khi consumer group **chưa có offset hợp lệ**.
- `earliest` → đọc từ message cũ nhất còn tồn tại.
- `latest` → bỏ qua message cũ, chờ message mới.

```
earliest:  0 → 1 → 2 → 3 → 4 → ...
           ↑ đọc từ đây

latest:    0 → 1 → 2 → 3 → 4 | → 5 → 6
                              ↑ chờ message mới
```

### 4.4. `acks`

- `0`: không chờ Kafka xác nhận.
- `1`: chờ xác nhận leader.
- `all`: chờ cả replica xác nhận.

## 5. Thiết kế Database & dựng base

> Hướng học: thiết kế các table sao cho hợp lý, dựng base cho service.

Ghi chú nhanh:
- Xác định đúng ranh giới nghiệp vụ (bounded context) trước khi thiết kế bảng.
- Mỗi service sở hữu schema riêng, không join trực tiếp sang DB service khác.
- Chuẩn hoá dữ liệu vừa đủ; cân nhắc denormalize cho các trường đọc nhiều.
- Đặt khoá chính, khoá ngoại (trong cùng service), index cho các trường truy vấn thường xuyên.
- Dùng migration (Flyway/Liquibase) để quản lý version schema.
- Dựng base: cấu trúc project, config, kết nối DB, migration, health check.

## 6. Lock & Retry

### 6.1. Lock

**Lock bi quan (Pessimistic Lock)**
- Khoá bản ghi trước khi thao tác, giữ lock tới khi transaction kết thúc.
- An toàn khi tranh chấp cao, nhưng giảm concurrency, dễ deadlock, giữ lock lâu.

**Lock lạc quan (Optimistic Lock)**
- Không khoá khi đọc; khi ghi kiểm tra version/CAS, nếu bị thay đổi thì báo lỗi và thử lại.
- Phù hợp tranh chấp thấp, cho throughput cao.

### 6.2. Retry

**Blocking retry**
- Luồng bị chặn trong lúc chờ và thử lại (sleep rồi retry).
- Đơn giản nhưng chiếm tài nguyên luồng, làm chậm luồng xử lý.

**Non-blocking retry**
- Không chặn luồng; đẩy vào queue/retry queue, worker xử lý lại sau.
- Phù hợp hệ thống lớn, tránh chiếm luồng.

## 7. Redis Lock

- Dùng Redis để tạo lock phân tán giữa nhiều instance/service.
- Cơ chế `SET key value NX PX <ttl>`: chỉ set khi key chưa tồn tại, kèm TTL để tránh deadlock.
- Giải phóng lock nên kiểm tra đúng chủ sở hữu (so sánh value) rồi mới xoá.
- Nếu cần an toàn cao, dùng thuật toán Redlock hoặc thư viện như Redisson.
