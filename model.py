import pika
import pickle
import json
import numpy as np

# Загрузка регрессионной модели
with open('myfile.pkl', 'rb') as pkl_file:
    regressor = pickle.load(pkl_file)

try:
    # Соединение с сервером localhost
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()

    # Создание очередей сообщений
    channel.queue_declare(queue='Features')
    channel.queue_declare(queue='y_pred')

    # Функция для отправки предсказанных ответов в очередь y_pred
    def callback(channel, method, property, body):
        print(f'Получен вектор признаков {body}')
        features_dict = json.loads(body)
        features = np.array(list(features_dict.values()))
        curr_id = int(list(features_dict.keys())[0])
        y_pred = regressor.predict(features)[0]
        channel.basic_publish(exchange='', routing_key='y_pred', body=json.dumps({curr_id: y_pred}))
        print(f'Сообщение с Id={curr_id} и предсказанием {y_pred} отправлено в очередь y_pred')


    # Ожидание сообщений из очереди queue_features
    channel.basic_consume(queue='Features', on_message_callback=callback, auto_ack=True)
    print('...Ожидание сообщений, для выхода нажмите CTRL+C')

    channel.start_consuming()
except:
    print('Не удалось подключиться к очереди')
