# Chức năng hệ thống

- quản lý hệ thống xe bus tại hà nội

# mô hình hoạt động 

<img width="821" alt="Screen Shot 2022-08-13 at 16 06 09" src="https://user-images.githubusercontent.com/43698423/184477870-5e7f2f3b-9529-4a35-9989-001e8bd6d063.png">

1. Kafka: stream dữ liệu đổ về

2. Spark Streaming: consum data vào lưu trữ data stream vào postgres

3. superset : Phân tích dữ liệu
