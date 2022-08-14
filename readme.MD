# Chức năng hệ thống

- khảo sát thông tin và phân tích về bệnh ung thư qua các năm

# mô hình hoạt động 

![Screen Shot 2022-08-14 at 23 51 49](https://user-images.githubusercontent.com/43698423/184547130-cb7b3782-970e-407b-a1e5-03a6dfa43577.png)

1. Kafka: stream dữ liệu đổ về

2. Spark Streaming: consum data vào lưu trữ data stream vào postgres

3. superset : Phân tích dữ liệu
