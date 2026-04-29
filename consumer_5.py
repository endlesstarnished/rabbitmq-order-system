import pika
import json
import time

class OrderConsumer:
    def __init__(self):
        # Подключиться к RabbitMQ
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters('localhost')
        )
        self.channel = self.connection.channel()
        
        # Объявить очередь 'orders'
        self.channel.queue_declare(queue='orders', durable=True)
        
        # Настроить prefetch_count=1
        self.channel.basic_qos(prefetch_count=1)
        
        # Для замера времени
        self.processed_count = 0
        self.start_time = None

    def callback(self, ch, method, properties, body):
        # Засечь время начала обработки первого заказа
        if self.processed_count == 0:
            self.start_time = time.time()
            print(f"Начало обработки первого заказа: {time.strftime('%H:%M:%S')}")
        
        # Распарсить JSON
        order_data = json.loads(body.decode())
        
        # Вывести информацию о заказе
        print(f"\nОбработка заказа: {order_data['order_id']}")
        print(f"Клиент: {order_data['customer']}")
        print(f"Сумма: {order_data['amount']} руб.")
        
        # Имитация обработки 2 секунды (ТРЕБОВАНИЕ ЗАДАНИЯ)
        print(f"Имитация обработки: 2 секунды...")
        time.sleep(2)
        
        # Вывести информацию о завершении обработки
        print(f"✅ Заказ {order_data['order_id']} обработан")
        
        # Увеличиваем счетчик обработанных заказов
        self.processed_count += 1
        print(f"📊 Обработано заказов: {self.processed_count}/5")
        
        # Если обработали 5 заказов - вывести итоговое время
        if self.processed_count == 5:
            end_time = time.time()
            total_time = end_time - self.start_time
            print("\n" + "="*50)
            print(f"✅ ОБРАБОТКА 5 ЗАКАЗОВ ЗАВЕРШЕНА")
            print(f"⏱️  Общее время обработки: {total_time:.2f} секунд")
            print(f"📊 Среднее время на заказ: {total_time/5:.2f} секунд")
            print(f"📝 Теоретическое время (5×2 сек): 10.00 секунд")
            print("="*50)
            
            # Сохраняем результаты в файл
            with open('time_results.txt', 'w', encoding='utf-8') as f:
                f.write("РЕЗУЛЬТАТЫ ЭКСПЕРИМЕНТА (с имитацией sleep 2 сек)\n")
                f.write("="*50 + "\n")
                f.write(f"Количество заказов: 5\n")
                f.write(f"Время обработки 5 заказов: {total_time:.2f} секунд\n")
                f.write(f"Среднее время на заказ: {total_time/5:.2f} секунд\n")
                f.write(f"Теоретическое время: 10.00 секунд\n")
                f.write(f"\nВывод:\n")
                f.write(f"- Consumer обрабатывает по одному заказу (prefetch_count=1)\n")
                f.write(f"- Каждый заказ обрабатывается 2 секунды (time.sleep(2))\n")
                f.write(f"- 5 заказов × 2 секунды = 10 секунд\n")
        
        # Подтвердить обработку (basic_ack)
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def start_consuming(self):
        # Запустить прослушивание очереди
        self.channel.basic_consume(
            queue='orders',
            on_message_callback=self.callback,
            auto_ack=False
        )
        print("="*50)
        print("Consumer запущен (с имитацией sleep 2 секунды)")
        print("Ожидание заказов...")
        print("="*50)
        self.channel.start_consuming()

if __name__ == '__main__':
    consumer = OrderConsumer()
    consumer.start_consuming()