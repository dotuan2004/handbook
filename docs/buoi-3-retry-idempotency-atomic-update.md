# Backend Notes: Retry, Idempotency và Atomic Update

## 1. Blocking Retry vs Non-blocking Retry trong Kafka

### 1.1. Blocking Retry

**Cơ chế:** Khi consumer xử lý message thất bại, consumer retry ngay tại luồng xử lý hiện tại. Trong thời gian retry, message tiếp theo chưa được xử lý bởi luồng đó.

Ví dụ:
- Message A xử lý thất bại.
- Consumer retry A sau 1 giây.
- Nếu A vẫn thất bại, tiếp tục retry.
- Sau khi xử lý xong A, consumer mới tiếp tục xử lý B.

**Ưu điểm:**
- Dễ triển khai, dễ kiểm soát luồng xử lý.
- Có thể giữ thứ tự xử lý message trong partition.
- Phù hợp với nghiệp vụ cần đảm bảo thứ tự giữa các message.

**Nhược điểm:**
- Message lỗi có thể chặn các message phía sau.
- Nếu một message liên tục thất bại, throughput của consumer giảm.
- Consumer có thể bị xử lý chậm, thậm chí gặp vấn đề về thời gian xử lý và rebalance nếu không kiểm soát tốt.

### 1.2. Non-blocking Retry

**Cơ chế:** Khi message thất bại, consumer chuyển message sang retry topic hoặc một cơ chế retry bất đồng bộ khác rồi tiếp tục xử lý message tiếp theo.

Ví dụ:
- Message A xử lý thất bại, được chuyển sang retry topic.
- Consumer tiếp tục xử lý B, C, D trên topic chính.
- Message A được xử lý lại sau đó.

**Ưu điểm:**
- Không cần giữ luồng xử lý chính chờ message lỗi.
- Tăng khả năng xử lý các message độc lập và cải thiện throughput.
- Hạn chế việc một message lỗi làm chậm toàn bộ luồng xử lý.

**Nhược điểm:**
- Có thể mất thứ tự xử lý giữa các message.
- Luồng retry phức tạp hơn, cần quản lý retry topic, số lần retry, backoff và DLQ.
- Phải xử lý cẩn thận việc publish sang retry topic và commit offset để tránh mất hoặc xử lý trùng message.

### 1.3. Vấn đề ordering

Giả sử cùng một đơn hàng có ba event:

- E1: OrderCreated
- E2: OrderPaid
- E3: OrderShipped

Nếu E1 bị lỗi và được chuyển sang retry topic, consumer vẫn có thể xử lý E2 và E3 trước khi E1 được xử lý thành công.

Điều này có thể khiến trạng thái nghiệp vụ bị sai thứ tự.

**Cách lựa chọn:**
- Nếu cần giữ thứ tự nghiêm ngặt: cân nhắc blocking retry hoặc cơ chế retry có kiểm soát thứ tự theo từng key.
- Nếu các message độc lập: non-blocking retry thường phù hợp hơn.
- Nếu chỉ cần giữ thứ tự theo từng đơn hàng: sử dụng `orderId` làm message key để các event của cùng đơn hàng vào cùng partition, đồng thời thiết kế retry sao cho không vi phạm thứ tự nghiệp vụ.

Lưu ý: Cùng partition không tự động đảm bảo thứ tự hoàn tất xử lý khi dùng non-blocking retry.

---

## 2. Idempotency trong Kafka

### 2.1. Idempotency là gì?

Idempotency là tính chất mà khi thực hiện cùng một thao tác nhiều lần, kết quả nghiệp vụ cuối cùng vẫn tương đương với việc thực hiện một lần.

Ví dụ:

Consumer nhận event `PaymentSucceeded` hai lần.

Nếu không có idempotency:
- Lần 1: cộng 100.000 đồng vào số dư.
- Lần 2: tiếp tục cộng 100.000 đồng.
- Kết quả: cộng tiền hai lần.

Nếu có idempotency:
- Lần 1: xử lý và ghi nhận event đã hoàn tất.
- Lần 2: nhận ra event đã được xử lý, bỏ qua thao tác nghiệp vụ.
- Kết quả: chỉ cộng tiền một lần.

### 2.2. Cách 1: Producer Idempotence

Kafka hỗ trợ producer idempotence thông qua cấu hình:

`enable.idempotence=true`

Cơ chế này sử dụng Producer ID (PID) và sequence number để giúp broker nhận diện các bản ghi được gửi lại do retry trong phạm vi cơ chế hỗ trợ của producer.

Ví dụ:
- Producer gửi một batch message.
- Broker ghi nhận batch đó.
- Producer không nhận được acknowledgement và gửi lại batch.
- Broker nhận diện bản gửi lại tương ứng và tránh ghi trùng do retry đó.

**Điểm cần nhớ:**
- Sequence number do Kafka producer quản lý, không phải một `eventId` nghiệp vụ do ứng dụng tự tạo.
- Producer idempotence giúp chống duplicate do retry ở phía producer trong phạm vi Kafka hỗ trợ.
- Nó không đảm bảo consumer không xử lý trùng nghiệp vụ.
- Nó không tự động chống việc ứng dụng gửi cùng một event nghiệp vụ hai lần bằng hai lần gọi `send()` độc lập.

Vì vậy, producer idempotence không thay thế consumer idempotency.

### 2.3. Cách 2: Consumer Idempotency

Ứng dụng chấp nhận rằng message có thể được giao hoặc xử lý lại và thiết kế consumer để không thực hiện nghiệp vụ nhiều lần.

Một cách phổ biến:
- Mỗi event có một `eventId` duy nhất.
- Consumer kiểm tra event đã được xử lý chưa.
- Nếu đã xử lý thành công, bỏ qua.
- Nếu chưa, thực hiện nghiệp vụ và ghi nhận kết quả.

Ví dụ bảng `processed_events`:

| event_id | status |
|---|---|
| EVT001 | COMPLETED |
| EVT002 | COMPLETED |

Khi nhận `EVT001` lần nữa, consumer nhận ra event đã hoàn tất và không thực hiện lại nghiệp vụ.

### 2.4. Vì sao Kafka consumer cần idempotency?

Một trường hợp phổ biến là consumer rebalance hoặc crash.

Ví dụ:
1. Consumer A nhận message M.
2. Consumer A xử lý nghiệp vụ thành công.
3. Consumer A chưa commit offset.
4. Partition được giao cho Consumer B do rebalance.
5. Consumer B đọc lại M.
6. Nếu không có idempotency, nghiệp vụ có thể bị thực hiện lần thứ hai.

Đây là một trong những lý do consumer cần được thiết kế để chịu được việc xử lý message lặp.

### 2.5. Lưu eventId ở đâu?

Có thể dùng Redis hoặc database, nhưng phải chọn theo yêu cầu nghiệp vụ.

**Redis:**
- Tra cứu nhanh.
- Có thể đặt TTL để tự động hết hạn dữ liệu.
- Phù hợp khi chấp nhận giới hạn thời gian chống trùng và đã thiết kế cơ chế xử lý lỗi phù hợp.

Hạn chế:
- Nếu key hết hạn quá sớm, message cũ được xử lý lại có thể gây duplicate.
- Nếu Redis mất dữ liệu hoặc trạng thái idempotency không còn, nghiệp vụ có thể bị thực hiện lại.
- Nếu đánh dấu event đã xử lý trước khi nghiệp vụ thành công, message có thể bị bỏ qua dù nghiệp vụ chưa hoàn tất.

**Database:**
- Có thể lưu `eventId` cùng với kết quả xử lý.
- Có thể dùng unique constraint để ngăn hai transaction cùng ghi nhận một event.
- Có thể đảm bảo tính nhất quán giữa trạng thái idempotency và dữ liệu nghiệp vụ nếu cả hai được cập nhật trong cùng transaction.

Đối với nghiệp vụ quan trọng như thanh toán, cập nhật số dư hoặc tạo đơn hàng, database thường là lựa chọn đáng cân nhắc hơn nếu cần tính nhất quán bền vững.

### 2.6. Điểm quan trọng nhất: Thứ tự xử lý và transaction

Không nên triển khai theo kiểu:

1. Kiểm tra event chưa tồn tại.
2. Thực hiện nghiệp vụ.
3. Ghi event vào Redis hoặc database.

Nếu hai consumer cùng nhận một event, cả hai có thể kiểm tra thấy event chưa tồn tại rồi cùng thực hiện nghiệp vụ.

Đây là race condition.

Giải pháp thường dùng:
- Tạo unique constraint trên `event_id`.
- Ghi nhận event và cập nhật dữ liệu nghiệp vụ trong cùng transaction database.
- Xử lý xung đột unique constraint như trường hợp event đã được xử lý hoặc đang được xử lý bởi transaction khác.

Nếu nghiệp vụ phải gọi hệ thống bên ngoài, chẳng hạn cổng thanh toán, cần thiết kế thêm idempotency key ở phía hệ thống đó hoặc cơ chế phối hợp phù hợp.

---

## 3. Atomic Update để tránh Race Condition

### 3.1. Vấn đề khi SELECT rồi UPDATE

Giả sử kho hàng còn 5 sản phẩm. Hai request cùng muốn mua 4 sản phẩm.

Cách xử lý không an toàn:

```sql
SELECT stock
FROM products
WHERE id = 1;
```

Sau đó, ứng dụng kiểm tra `stock >= quantity` rồi chạy:

```sql
UPDATE products
SET stock = stock - 4
WHERE id = 1;
```

Một tình huống có thể xảy ra:

1. Request A đọc stock = 5.
2. Request B cũng đọc stock = 5.
3. A kiểm tra thấy đủ hàng.
4. B cũng kiểm tra thấy đủ hàng.
5. A trừ 4, còn 1.
6. B tiếp tục trừ 4 dựa trên kết quả kiểm tra cũ.

Nếu không có cơ chế đồng bộ phù hợp, stock có thể thành -3.

Nguyên nhân là thao tác kiểm tra điều kiện và cập nhật dữ liệu không được bảo vệ như một đơn vị xử lý nguyên tử.

### 3.2. Atomic Update là gì?

Atomic update là cách thực hiện thao tác cập nhật dữ liệu như một đơn vị nguyên tử, tránh để các request khác chen vào giữa bước kiểm tra và cập nhật theo cách làm sai nghiệp vụ.

Có thể gộp điều kiện kiểm tra và cập nhật thành một câu SQL:

```sql
UPDATE products
SET stock = stock - 4
WHERE id = 1
  AND stock >= 4;
```

Sau đó kiểm tra số dòng bị ảnh hưởng:

- `rows affected = 1`: cập nhật thành công.
- `rows affected = 0`: sản phẩm không tồn tại hoặc không đủ hàng.

Trong trường hợp hai request cùng mua 4 sản phẩm khi stock ban đầu bằng 5, chỉ một request có thể cập nhật thành công. Request còn lại không thể trừ tiếp 4 sản phẩm vì điều kiện `stock >= 4` không còn đúng.

### 3.3. Vì sao Atomic Update giúp tránh Race Condition?

Database thực hiện câu lệnh cập nhật trên dữ liệu hiện tại và áp dụng cơ chế đồng bộ thích hợp để xử lý các transaction cạnh tranh.

Khi hai request đồng thời cập nhật cùng một dòng:
- Một request có thể phải chờ khóa của request còn lại.
- Sau khi request trước hoàn tất, request sau tiếp tục đánh giá điều kiện trên trạng thái dữ liệu phù hợp với cơ chế concurrency control của database.
- Nếu điều kiện không còn đúng, câu lệnh không cập nhật dòng đó.

Điểm quan trọng: **Atomic Update không phải chỉ vì transaction có ACID hoặc isolation level.** Nó còn dựa vào cách database thực thi câu lệnh và cơ chế khóa/kiểm soát đồng thời.

Gộp điều kiện và cập nhật vào một câu lệnh giúp loại bỏ khoảng thời gian giữa bước đọc dữ liệu và bước cập nhật ở phía ứng dụng.

### 3.4. Hạn chế của Atomic Update

Nếu câu SQL có nhiều điều kiện, khi `rows affected = 0`, khó xác định ngay nguyên nhân cụ thể.

Ví dụ:

```sql
UPDATE products
SET stock = stock - :quantity
WHERE id = :productId
  AND status = 'ACTIVE'
  AND stock >= :quantity;
```

Câu lệnh có thể không cập nhật dòng nào vì:
- Product không tồn tại.
- Product đã bị vô hiệu hóa.
- Stock không đủ.

Kết quả `rows affected = 0` không trực tiếp cho biết điều kiện nào thất bại.

**Cách xử lý:**
- Nếu nghiệp vụ chỉ cần biết thành công hay thất bại, kiểm tra số dòng bị ảnh hưởng là đủ.
- Nếu cần trả thông báo lỗi cụ thể, có thể đọc và kiểm tra trạng thái sau khi cập nhật thất bại, hoặc dùng transaction và locking phù hợp để bảo đảm tính nhất quán của việc kiểm tra.
- Tránh SELECT trước rồi UPDATE sau mà không có cơ chế đồng bộ, vì có thể tái tạo race condition.

### 3.5. Khi nào nên dùng Atomic Update?

Phù hợp với các thao tác có điều kiện rõ ràng:
- Trừ stock khi đặt hàng.
- Trừ số lượt thi thử còn lại.
- Giảm số lượng chỗ còn trống.
- Tăng bộ đếm.
- Chuyển trạng thái khi trạng thái hiện tại đáp ứng điều kiện.

Ví dụ trừ số lượt thi thử:

```sql
UPDATE user_entitlements
SET remaining_uses = remaining_uses - 1
WHERE user_id = :userId
  AND product_code = :productCode
  AND remaining_uses > 0;
```

Nếu chỉ một dòng được cập nhật, thao tác tiêu thụ lượt thành công. Nếu không có dòng nào được cập nhật, không thể kết luận ngay nguyên nhân là hết lượt; cần xem xét các điều kiện khác như user hoặc product không tồn tại.

---

## 4. Tổng kết: Ba nguyên tắc cần nhớ

### Retry
- Blocking retry: dễ giữ thứ tự xử lý nhưng message lỗi có thể chặn các message sau.
- Non-blocking retry: cải thiện khả năng xử lý song song các message độc lập nhưng có thể phá vỡ thứ tự hoàn tất nghiệp vụ.

### Idempotency
- Producer idempotence không thay thế consumer idempotency.
- Kafka consumer có thể xử lý lại message do crash hoặc rebalance.
- `eventId` giúp nhận diện message trùng, nhưng phải kết hợp với cơ chế lưu trữ và đồng bộ phù hợp.
- Với nghiệp vụ quan trọng, ưu tiên bảo đảm tính nhất quán giữa trạng thái idempotency và cập nhật dữ liệu nghiệp vụ.

### Atomic Update
- SELECT rồi UPDATE có thể tạo race condition nếu không có cơ chế đồng bộ phù hợp.
- Gộp điều kiện và cập nhật vào một câu SQL giúp database kiểm tra và cập nhật dữ liệu một cách nguyên tử.
- `rows affected = 0` chỉ cho biết không có dòng nào được cập nhật, không nhất thiết cho biết chính xác điều kiện nào thất bại.
