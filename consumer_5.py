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
        self.end_time = None

    def callback(self, ch, method, properties, body):
        # Засечь время начала обработки первого заказа
        if self.processed_count == 0:
            self.start_time = time.time()
            print(f"Начало обработки первого заказа: {time.strftime('%H:%M:%S')}")
        
        # Распарсить JSON
        order_data = json.loads(body.decode())
        
        # Засечь время начала обработки этого заказа
        order_start_time = time.time()
        
        # Вывести информацию о заказе (без имитации sleep)
        print(f"Обработка заказа: {order_data['order_id']}")
        
        # Просто выводим информацию - это и есть "обработка"
        print(f"Заказ обработан: ID={order_data['order_id']}, "
              f"Клиент={order_data['customer']}, "
              f"Сумма={order_data['amount']}")
        
        # Засечь время окончания обработки этого заказа
        order_end_time = time.time()
        order_processing_time = order_end_time - order_start_time
        print(f"Время обработки этого заказа: {order_processing_time:.3f} сек")
        
        # Увеличиваем счетчик обработанных заказов
        self.processed_count += 1
        print(f"Обработано заказов: {self.processed_count}/5")
        
        # Если обработали 5 заказов, засечь время и вывести результат
        if self.processed_count == 5:
            self.end_time = time.time()
            total_time = self.end_time - self.start_time
            print("\n" + "="*50)
            print(f"ОБРАБОТКА 5 ЗАКАЗОВ ЗАВЕРШЕНА")
            print(f"Время начала: {time.strftime('%H:%M:%S', time.localtime(self.start_time))}")
            print(f"Время окончания: {time.strftime('%H:%M:%S', time.localtime(self.end_time))}")
            print(f"Общее время обработки: {total_time:.3f} секунд")
            print(f"Среднее время на заказ: {total_time/5:.3f} секунд")
            print("="*50)
            
            # Сохраняем результаты в файл
            with open('time_results.txt', 'w') as f:
                f.write(f"РЕЗУЛЬТАТЫ ЭКСПЕРИМЕНТА (без sleep)\n")
                f.write(f"Время обработки 5 заказов: {total_time:.3f} секунд\n")
                f.write(f"Начало: {time.strftime('%H:%M:%S', time.localtime(self.start_time))}\n")
                f.write(f"Окончание: {time.strftime('%H:%M:%S', time.localtime(self.end_time))}\n")
                f.write(f"Общее время: {total_time:.3f} сек\n")
                f.write(f"Среднее на заказ: {total_time/5:.3f} сек\n")
                f.write(f"\nАнализ:\n")
                f.write(f"- Без имитации sleep(2) время обработки значительно меньше\n")
                f.write(f"- Показывает реальную скорость работы RabbitMQ\n")
                f.write(f"- Основное время - передача сообщений, а не обработка\n")
        
        # Подтвердить обработку (basic_ack)
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def start_consuming(self):
        # Запустить прослушивание очереди
        self.channel.basic_consume(
            queue='orders',
            on_message_callback=self.callback,
            auto_ack=False
        )
        self.channel.start_consuming()

if __name__ == '__main__':
    consumer = OrderConsumer()
    print("Ожидание заказов...")
    print("Consumer готов обрабатывать заказы (без имитации sleep)")
    print("Для эксперимента отправьте 5 заказов через producer")
    consumer.start_consuming()