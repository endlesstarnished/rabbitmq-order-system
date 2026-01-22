import pika
import json
import time
from datetime import datetime

class OrderProducer:
    def __init__(self):
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters('localhost')
        )
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue='orders', durable=True)

    def send_order(self, order_id, customer, amount):
        message = {
            "order_id": order_id,
            "customer": customer,
            "amount": amount,
            "status": "pending"
        }
        
        self.channel.basic_publish(
            exchange='',
            routing_key='orders',
            body=json.dumps(message),
            properties=pika.BasicProperties(delivery_mode=2)
        )
        
        send_time = datetime.now().strftime('%H:%M:%S.%f')[:-3]
        print(f"[{send_time}] Отправлен: {order_id}")
        return send_time

    def close(self):
        self.connection.close()

if __name__ == '__main__':
    producer = OrderProducer()
    print("Отправляю 5 заказов БЫСТРО (без задержек)...")
    
    start_time = time.time()
    
    # Отправляем 5 заказов БЕЗ задержек
    orders = [
        ("ORD-101", "Алексей Смирнов", 1500),
        ("ORD-102", "Мария Иванова", 2500),
        ("ORD-103", "Сергей Петров", 3500),
        ("ORD-104", "Ольга Сидорова", 4500),
        ("ORD-105", "Дмитрий Кузнецов", 5500)
    ]
    
    for order_id, customer, amount in orders:
        producer.send_order(order_id, customer, amount)
    
    producer.close()
    print(f"\nВсе 5 заказов отправлены за {time.time() - start_time:.3f} сек")