import numpy as np
import pika
import json
from sklearn.datasets import load_diabetes
import time
import uuid

# Загрузка датасета
X, y = load_diabetes(return_X_y=True)

# Отправление сообщений в нужные очереди в бесконечном цикле
while True:
    try:
        # Выбор нового случайного объекта из датасета
        random_row = np.random.randint(0, X.shape[0] - 1)
        # Создание уникального id для сообщений по каждому объекту
        message_id = uuid.uuid1().int

        # Соединение с сервером localhost
        connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        channel = connection.channel()

        # Создание очередей сообщений
        channel.queue_declare(queue='y_true')
        channel.queue_declare(queue='Features')

        channel.basic_publish(exchange='', routing_key='y_true', body=json.dumps({message_id: y[random_row]}))
        print(f'Сообщение с id={message_id} и правильным ответом {y[random_row]} отправлено в очередь queue_y_true')

        channel.basic_publish(exchange='', routing_key='Features', body=json.dumps({message_id: list(X[random_row])}))
        print(f'Сообщение с id={message_id} и признаками {list(X[random_row])} отправлено в очередь queue_features')

        connection.close()

        # Задержка в 10 секунд после каждой итерации
        time.sleep(10)
    except:
        print('Не удалось подключиться к очереди')
