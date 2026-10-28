# Buổi 2 — Tổng hợp kiến thức Kafka

## 1. Graceful Shutdown

### 1.1. Khái niệm

Graceful shutdown là tắt service một cách có kiểm soát, thay vì dừng đột ngột.

### 1.2. Với Kafka Consumer

Khi shutdown, cần:

- Ngừng nhận thêm công việc mới.
- Chờ xử lý các message đang chạy trong giới hạn timeout.
- Commit offset sau khi xử lý thành công, theo cơ chế commit đang sử dụng.
- Đóng consumer để Kafka phát hiện việc rời khỏi consumer group và thực hiện rebalance.

> Nếu xử lý message xong nhưng chưa commit offset, khi consumer khác nhận partition đó, message có thể bị xử lý lại. Vì vậy, cần thiết kế consumer theo hướng idempotent.

### 1.3. Với Kafka Producer

- Flush các bản tin đang chờ gửi.
- Đóng producer đúng cách để giảm nguy cơ mất dữ liệu đang nằm trong buffer.

### 1.4. Lưu ý

Graceful shutdown không tự đảm bảo xử lý đúng một lần (exactly-once). Nó chỉ giảm thiểu rủi ro mất mát hoặc xử lý trùng.

---

## 2. Tăng số partition có thể làm thay đổi partition theo key

### 2.1. Cơ chế phân vùng theo key

Giả sử bạn có 4 partition: P0, P1, P2, P3. Producer gửi message có key là `user-123`. Với cách phân vùng thông thường, partition được chọn dựa trên hash của key và số partition hiện tại:

```
partition = hash(key) mod N
```

- Khi `N=4`, key có thể được đưa vào P1.
- Khi tăng lên `N=5`, cùng key đó có thể được đưa vào P4.

> Đây là ví dụ minh họa; kết quả thực tế phụ thuộc thuật toán partitioner và giá trị hash.

### 2.2. Hệ quả

Các message mới cùng key có thể được gửi vào partition khác với các message cũ. Kafka không còn đảm bảo thứ tự xử lý toàn cục cho key đó giữa các partition.

> Nếu nghiệp vụ yêu cầu giữ thứ tự (ví dụ: tất cả sự kiện của cùng một đơn hàng phải xử lý đúng trình tự), cần cân nhắc kỹ trước khi tăng partition.

---

## 3. Consumer Rebalance

### 3.1. Khái niệm

Rebalance xảy ra khi Kafka cần phân phối lại partition giữa các consumer trong cùng một consumer group — ví dụ khi consumer chết, consumer mới tham gia, hoặc cấu hình subscription thay đổi.

### 3.2. Các hệ lụy

| Hệ lụy | Mô tả |
|--------|-------|
| Tạm dừng xử lý | Consumer phải chờ việc phân phối partition hoàn tất |
| Message bị xử lý lại | Consumer xử lý thành công nhưng chưa commit offset → consumer mới có thể đọc lại |
| Retry làm tăng tải | Nếu rebalance đi kèm lỗi xử lý hoặc timeout, cơ chế retry khiến hệ thống xử lý cùng nghiệp vụ nhiều lần |
| Độ trễ tăng | Backlog tích lũy trong thời gian consumer không xử lý bình thường |

> Không phải cứ rebalance là message sẽ bị retry. Rebalance và retry là hai cơ chế khác nhau; việc xử lý lại thường liên quan đến offset, commit và cách ứng dụng quản lý lỗi.

### 3.3. Cách hạn chế

- Dùng graceful shutdown khi triển khai phiên bản mới.
- Cấu hình thời gian xử lý, polling và heartbeat hợp lý.
- Dùng idempotency cho các nghiệp vụ có side effect (tạo đơn hàng, trừ tiền).
- Cân nhắc cooperative/incremental rebalance để giảm việc thu hồi toàn bộ partition.

---

## 4. Cấu hình `acks`

### 4.1. Các mức acks

| Cấu hình | Cơ chế | Ưu điểm | Rủi ro |
|----------|--------|----------|--------|
| `acks=0` | Producer không chờ broker xác nhận | Độ trễ thấp | Không biết broker đã nhận message hay chưa |
| `acks=1` | Chờ leader ghi nhận | Cân bằng độ trễ và độ bền dữ liệu | Leader chết trước khi follower đồng bộ → mất dữ liệu đã xác nhận |
| `acks=all` | Chờ toàn bộ replica trong ISR xác nhận | Độ bền dữ liệu cao hơn | Độ trễ cao hơn; từ chối ghi khi ISR không đủ |

### 4.2. Chọn theo nghiệp vụ

- **Log debug, metric không quan trọng** → cân nhắc `acks=0` nếu chấp nhận mất dữ liệu.
- **Event ít quan trọng, ưu tiên độ trễ** → có thể dùng `acks=1` nếu chấp nhận rủi ro mất dữ liệu một số tình huống.
- **Đơn hàng, thanh toán, quyền sử dụng gói học** → ưu tiên `acks=all`, kết hợp replication factor và `min.insync.replicas` phù hợp.

> `acks=all` không có nghĩa là chờ mọi replica được cấu hình cho partition. Nó chờ mọi replica hiện đang nằm trong ISR. `min.insync.replicas` mới là ngưỡng tối thiểu để Kafka chấp nhận ghi với `acks=all`.

---

## 5. `min.insync.replicas`

### 5.1. Khái niệm

`min.insync.replicas` đếm số replica đồng bộ của **từng partition**, bao gồm cả leader — không phải tổng số broker của cluster.

### 5.2. Ví dụ minh họa

Cluster có 4 broker, topic có 3 partition, replication factor = 3:

- Mỗi partition có 3 replica.
- Tổng số replica của cả topic là 9, nhưng không có nghĩa một partition có 9 replica.
- Nếu cấu hình `min.insync.replicas=3`, từng partition cần ít nhất 3 replica trong ISR để chấp nhận ghi với `acks=all`.

> Nếu đặt `min.insync.replicas=5` trong khi mỗi partition chỉ có RF = 3, ngưỡng thực tế Kafka áp dụng không thể vượt quá số replica của partition. Cấu hình này không đem lại lợi ích so với đặt ngưỡng 3 và rất dễ gây hiểu nhầm.

---

## 6. Tình huống thực tế: 4 broker, một broker chết trong 2 tiếng

### 6.1. Giả sử

- Cluster có 4 broker: B1, B2, B3, B4.
- Topic có 3 partition: P0, P1, P2.
- RF = 3, `min.insync.replicas=3`, producer dùng `acks=all`.
- B1 chết trong 2 tiếng.

### 6.2. Ví dụ minh họa vị trí replica

Vì RF = 3, mỗi partition có 3 replica. Kafka phân bổ replica trên các broker khác nhau. Ví dụ:

| Partition | Leader | Replica 2 | Replica 3 |
|-----------|--------|-----------|-----------|
| P0        | B1     | B2        | B3        |
| P1        | B2     | B3        | B4        |
| P2        | B3     | B4        | B1        |

> Đây là ví dụ minh họa; cách phân bổ thực tế phụ thuộc thuật toán của Kafka. Mỗi broker đều chứa replica của ít nhất 1 partition.

### 6.3. Trường hợp B1 chết

Khi B1 chết:

| Partition | Replica trên B1 | Trạng thái | ISR còn lại |
|-----------|-----------------|------------|-------------|
| P0        | Leader          | Mất leader | B2, B3 (2/3) |
| P1        | Không có        | Không ảnh hưởng | B2 (Leader), B3, B4 (3/3) |
| P2        | Replica sao chép | Mất replica | B3 (Leader), B4 (2/3) |

**Hệ quả:**

- **P0:** Mất leader. Kafka sẽ chọn leader mới từ các replica còn sống (B2 hoặc B3). Tạm thời P0 không thể nhận ghi cho tới khi leader mới được bầu.
- **P1:** Không có replica trên B1, vẫn đủ 3 replica trong ISR (B2, B3, B4) → **vẫn nhận ghi bình thường**.
- **P2:** Mất 1 replica, còn 2 replica trong ISR (B3, B4) → **không thể nhận ghi** với `acks=all` và `min.insync.replicas=3`.

### 6.4. Bảng tổng hợp trạng thái các partition

| Partition | ISR trước khi B1 chết | ISR sau khi B1 chết | Nhận ghi được? |
|-----------|----------------------|---------------------|----------------|
| P0        | B1, B2, B3 (3/3)     | B2, B3 (2/3)        | ❌ Không        |
| P1        | B2, B3, B4 (3/3)     | B2, B3, B4 (3/3)    | ✅ Có           |
| P2        | B3, B4, B1 (3/3)     | B3, B4 (2/3)        | ❌ Không        |

> Với `min.insync.replicas=3`, chỉ P1 vẫn đủ điều kiện nhận ghi. P0 và P2 đều không thể chấp nhận ghi mới cho tới khi có replica bổ sung trở lại ISR.

### 6.5. Hệ lụy trong 2 tiếng

1. **Một số message không ghi được vào Kafka.** Với P0 và P2, producer gửi message nhưng Kafka từ chối. Producer có thể retry; nếu hết thời gian hoặc hết số lần retry, ứng dụng nhận lỗi gửi message.
2. **Không nhất thiết cả 3 partition đều lỗi.** Partition nào mất một replica và ISR giảm dưới ngưỡng thì partition đó không chấp nhận ghi. Partition khác vẫn đủ ISR thì vẫn ghi bình thường.
3. **Dữ liệu không tự động mất chỉ vì producer bị từ chối ghi.** Nhưng nếu ứng dụng bỏ qua lỗi hoặc không lưu lại message để gửi lại, nghiệp vụ có thể mất sự kiện.
4. **Consumer có thể vẫn đọc được dữ liệu cũ.** Việc từ chối ghi mới không đồng nghĩa toàn bộ dữ liệu hiện có không đọc được.

### 6.6. Bốn broker có nghĩa là B4 tự thay thế B1 không?

Không. Kafka không tự biến B4 thành replica của P0 hoặc P2 ngay lập tức chỉ vì B1 chết. Việc khôi phục replica cần cơ chế phân bổ lại replica và đồng bộ dữ liệu.

Nếu B1 quay lại sau 2 tiếng, replica của B1 cần bắt kịp leader và đáp ứng điều kiện để trở lại ISR. Khi ISR của P0 và P2 đạt lại 3, các partition đó có thể tiếp tục nhận ghi.

---

## 7. Kafka vs RabbitMQ — khi nào chọn cái nào

### 7.1. Điểm nổi bật của Kafka

Kafka phù hợp khi cần:

- **Replay dữ liệu lịch sử:** message được lưu trên log theo retention, consumer có thể đọc lại từ offset cũ.
- **Nhiều Consumer Group đọc độc lập:** mỗi group có offset riêng, không ảnh hưởng nhau.
- **Throughput rất cao:** ghi/đọc tuần tự trên log, phù hợp luồng sự kiện lớn.

### 7.2. Cơ chế scale

| | Kafka | RabbitMQ |
|---|-------|----------|
| Cách scale | Tăng **partition** (song song hóa theo partition, thêm consumer trong group) | Tăng **broker/node** trong cluster |
| Giới hạn song song | Số consumer hữu ích trong một group ≤ số partition | Phụ thuộc số queue và số consumer của từng queue |

> Tăng partition cũng kéo theo hệ quả đã nêu ở mục 2: key có thể bị đưa sang partition khác, ảnh hưởng thứ tự theo key.

### 7.3. Throughput và tính nhất quán

- **Kafka** xử lý tốt lượng throughput lớn.
- **RabbitMQ** là mô hình queue đơn giản (broker đẩy message tới consumer, ack xong thì xóa), nên dễ hiểu và dễ đảm bảo tính nhất quán hơn cho các luồng nhỏ, cần xử lý từng việc một.

> Ghi chú để nhớ: "nhất quán hơn" ở đây hiểu theo nghĩa **mô hình đơn giản, dễ suy luận**, không phải RabbitMQ tự đảm bảo exactly-once. Cả hai đều cần consumer idempotent (xem mục 1 và 3).

### 7.4. Tóm tắt chọn nhanh

| Nhu cầu | Nên cân nhắc |
|---------|--------------|
| Replay lịch sử, event streaming, nhiều group đọc độc lập, throughput lớn | Kafka |
| Task queue / job đơn giản, routing linh hoạt, khối lượng vừa phải | RabbitMQ |
