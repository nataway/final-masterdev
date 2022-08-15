# Chức năng hệ thống

- khảo sát thông tin và phân tích về bệnh tim mạch qua các năm

# mô hình hoạt động 

<img width="821" alt="Screen Shot 2022-08-13 at 16 06 09" src="https://user-images.githubusercontent.com/43698423/184611087-d0c4b70a-384a-4107-9764-76dbfee232f0.png">

1. Kafka: stream dữ liệu đổ về

2. Spark Streaming: consum data vào lưu trữ data stream vào postgres

3. superset : Phân tích dữ liệu
