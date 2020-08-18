import pika
import json
import os
import numpy as np
from sklearn.metrics import mean_squared_error

try:
    # Создание подключения к серверу localhost
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()

    # Объявление очередей сообщений
    channel.queue_declare(queue='y_true')
    channel.queue_declare(queue='y_pred')


    # Добавление значения в файл, только если в нем менее 2 строк (1 для y_pred, 1 для y_true)
    def write_to_file(file_name, queue_name, value):
        with open(file_name, 'a+') as file:
            lines = file.read().splitlines()
            if len(lines) < 2:
                file.write(f"{queue_name},{value}")
                file.write("\n")


    # Функция расчета RMSE для данных, по которым пришли и истинный ответ, и предсказание
    def count_rmse():
        rmse = []
        predictions = []
        true_values = []
        for filename in os.listdir('data_files'):
            with open(os.path.join('data_files', filename), 'r') as file:  # readonly
                lines = file.read().splitlines()
                if len(lines) == 2:
                    for line in lines:
                        fields = line.split(',')
                        if fields[0] == 'y_true':
                            true_values.append(float(fields[1]))
                        else:
                            predictions.append(float(fields[1]))

        if len(predictions) != 0:
            rmse.append(mean_squared_error(true_values, predictions, squared=False))
            print(f'Обновленное значение RMSE на {len(predictions)} объектах равно {rmse[len(rmse) - 1]}')
        else:
            print(f'Для RMSE недостаточно данных')


    # Функция для агрегирования данных по предсказаниям и истинным ответам
    def callback(ch, method, property, body):
        data = json.loads(body)
        message_value = list(data.values())[0]
        curr_id = int(list(data.keys())[0])
        print(f'Из очереди {method.routing_key} получено значение {message_value} для ID={curr_id}')
        file_name = os.path.join('data_files', str(curr_id) + ".txt")
        write_to_file(file_name, method.routing_key, message_value)
        count_rmse()


    # Ожидание сообщений из очередей и перенаправление в функцию callback
    channel.basic_consume(
        queue='y_pred', on_message_callback=callback, auto_ack=True)
    channel.basic_consume(
        queue='y_true', on_message_callback=callback, auto_ack=True)

    print('...Ожидание сообщений, для выхода нажмите CTRL+C')
    channel.start_consuming()
except:
    print('Не удалось подключиться к очереди')
