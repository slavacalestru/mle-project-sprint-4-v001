# Импортируем необходимые библиотеки
import sys
import requests
from IPython.display import display
from s3_utils import load_parquet_from_s3

# Создаем класс Tee для записи данных одновременно в терминал и файл
class Tee:
    # Инициализируем класс с указанием пути к файлу для записи и потока для вывода данных
    def __init__(self, filepath, stream):
        self.file = open(filepath, "w")
        self.stream = stream

    # Метод для записи данных в файл и поток. Данные записываются в файл и выводятся в терминал.
    def write(self, data):
        self.stream.write(data)
        self.file.write(data)

    # Метод для очистки буфера
    def flush(self):
        self.stream.flush()
        self.file.flush()

# Перенаправляем стандартные потоки вывода чтобы данные записывались одновременно в терминал и файл "test_service.log"
sys.stdout = Tee("test_service.log", sys.stdout)
sys.stderr = Tee("test_service.log", sys.stderr)

# Указываем URL для доступа к сервисам фичей, событий и рекомендаций
feature_store_url = "http://0.0.0.0:8000"
events_store_url = "http://0.0.0.0:8001"
recommendations_url = "http://0.0.0.0:8002"

# Задаем заголовки для HTTP-запросов, указывая тип контента и формат ответа
headers = {"Content-Type": "application/json", "Accept": "text/plain"}

# Загружаем данные о товарах из S3 бакета для использования в тестах
items = load_parquet_from_s3("items.parquet")

# Определяем функцию для тестирования работы сервиса фичей
def test_features_store_service(item_id=99262, k=5):
    params = {"item_id": item_id, "k":k}

    # Отправляем POST-запрос к эндпоинту "/similar_items"
    resp = requests.post(feature_store_url + "/similar_items", headers=headers, params=params)

    if resp.status_code == 200:
        similar_items = resp.json()
    else:
        similar_items = None
        print(f"status code: {resp.status_code}, response: {resp.text}")
    # Выводим полученные похожие товары или сообщение об ошибке
    print(similar_items)


# Определяем функцию для тестирования работы сервиса событий
def test_save_interaction_to_events_store(item_id, user_id=19532):
    params = {"user_id": user_id, "item_id": item_id}

    # Отправляем POST-запрос к эндпоинту "/put" для сохранения взаимодействия пользователя с товаром
    resp = requests.post(events_store_url + "/put", headers=headers, params=params)

    if resp.status_code == 200:
        result = resp.json()
    else:
        result = None
        print(f"status code: {resp.status_code}, response: {resp.text}")

    # Выводим результат сохранения взаимодействия или сообщение об ошибке
    print(result)


# Определяем функцию для тестирования получения взаимодействий пользователя из сервиса событий
def test_get_interactions_from_events_store(user_id=19532):
    params = {"user_id": user_id}

    # Отправляем POST-запрос к эндпоинту "/get" для получения последних взаимодействий пользователя
    resp = requests.post(events_store_url + "/get", headers=headers, params=params)

    if resp.status_code == 200:
        events = resp.json()
    else:
        events = None
        print(f"status code: {resp.status_code}, response: {resp.text}")

    # Выводим полученные взаимодействия пользователя или сообщение об ошибке
    print(events)


# Определяем функцию для тестирования получения онлайн рекомендаций для пользователя
def test_online_recommendations(user_id=20153, k=3):
    params = {"user_id": user_id, "k": k}
    # Отправляем POST-запрос к эндпоинту "/recommendations_online" для получения онлайн рекомендаций для пользователя
    resp = requests.post(recommendations_url + "/recommendations_online", headers=headers, params=params)
    online_recs = resp.json()
    # Выводим полученные онлайн рекомендации для пользователя
    print(online_recs)


# Определяем функцию для добавления нескольких взаимодействий для пользователя в сервис событий
def add_interactions_for_user(user_id=20153, item_ids=[589498, 590262, 97220301]):
    # Для каждого item_id из списка item_ids вызываем функцию test_save_interaction_to_events_store
    for item_id in item_ids:
        test_save_interaction_to_events_store(item_id=item_id, user_id=user_id)


# Определяем функцию для отображения информации о товарах по их идентификаторам
def display_items(item_ids):
    item_columns_to_use = ["item_id", "track_name", "artist_names", "genre_names"]
    items_selected = items.query("item_id in @item_ids")[item_columns_to_use]
    items_selected = items_selected.set_index("item_id").reindex(item_ids)
    items_selected = items_selected.reset_index()
    display(items_selected)


# Определяем функцию для тестирования получения рекомендаций для пользователя без персональных рекомендаций. Ожидается, что пользователь получит дефолтные рекомендации.
def test_recommendations_no_personal(user_id=999999999, k=10):
    params = {"user_id": user_id, "k": k}
    # Отправляем POST-запрос к эндпоинту "/recommendations" для получения рекомендаций для пользователя без персональных рекомендаций
    resp = requests.post(recommendations_url + "/recommendations", headers=headers, params=params)
    recs = resp.json()["recs"]
    # Выводим полученные рекомендации для пользователя без персональных рекомендаций. 
    print(f"Рекоммендации для юзера {user_id} (нет персональных рекомендаций): {recs}")
    if recs:
        print("Получены дефолтные рекоммендации:")
        display_items(recs)
    else:
        print("Рекомендации не получены")


# Определяем функцию для тестирования получения рекомендаций для пользователя с персональными рекомендациями, но без онлайн истории.
def test_recommendations_personal_no_online(user_id=1374582, k=10):
    params = {"user_id": user_id, "k": k}
    # Получаем оффлайн рекомендации, онлайн рекомендации и комбинированные рекомендации для пользователя
    resp_offline = requests.post(recommendations_url + "/recommendations_offline", headers=headers, params=params)
    resp_online = requests.post(recommendations_url + "/recommendations_online", headers=headers, params=params)
    resp = requests.post(recommendations_url + "/recommendations", headers=headers, params=params)

    # Извлекаем рекомендации из ответов и выводим их
    recs_offline = resp_offline.json()["recs"]
    recs_online = resp_online.json()["recs"]
    recs = resp.json()["recs"]

    print(f"Оффлайн рекомендации: {recs_offline}")
    print(f"Онлайн рекоммендации (Должно быть пусто): {recs_online}")
    print(f"Комбинированные рекомендации (нет онлайн истории): {recs}")
    print("Персонализированные онлайн рекомендации:")
    display_items(recs_offline)
    print()
    print("Комбинированные рекомендации (Должны быть такие же как оффлайн, поскольку нет онлайн ивентов):")
    display_items(recs)


# Определяем функцию для тестирования получения рекомендаций для пользователя с персональными рекомендациями и онлайн историей
def test_combined_recommendations(user_id=20153, k=10, event_item_ids=[589498, 590262, 97220301]):

    # Добавляем несколько взаимодействий для пользователя в сервис событий
    add_interactions_for_user(user_id=user_id, item_ids=event_item_ids)

    # Получаем оффлайн рекомендации, онлайн рекомендации и комбинированные рекомендации для пользователя
    params = {"user_id": user_id, "k": k}
    resp_offline = requests.post(recommendations_url + "/recommendations_offline", headers=headers, params=params)
    resp_online = requests.post(recommendations_url + "/recommendations_online", headers=headers, params=params)
    resp_combined = requests.post(recommendations_url + "/recommendations", headers=headers, params=params)

    # Извлекаем рекомендации из ответов и выводим их
    recs_offline = resp_offline.json()["recs"]
    recs_online = resp_online.json()["recs"]
    recs_combined = resp_combined.json()["recs"]

    # Выводим оффлайн, онлайн и комбинированные рекомендации для пользователя
    print(f"Оффлайн рекомендации: {recs_offline}")
    print(f"Онлайн рекомендации: {recs_online}")
    print(f"Комбиринованные рекомендации: {recs_combined}\n")

    # Выводим информацию о событиях пользователя, оффлайн рекомендациях, онлайн рекомендациях и комбинированных рекомендациях для проверки их адекватности
    print("Проверяем общую адекватность рекомендаций")
    print("Онлайн-события")
    display_items(event_item_ids)
    print()
    print("Офлайн-рекомендации")
    display_items(recs_offline)
    print()
    print("Онлайн-рекомендации")
    display_items(recs_online)
    print()
    print("Рекомендации")
    display_items(recs_combined)
    print()



# Тест 1 проверяет работу сервиса фичей
print("\n===== ТЕСТ 1: Сервис фичей =====")
print("Тестируем сервис фичей с item_id=99262 и k=5")
test_features_store_service()
print("====================\n")


# Тест 2 проверяет работу сервиса событий
print("===== ТЕСТ 2: Сервис событий=====")
print("Тестируем сервис событий с user_id=19532 и несколькими item_id")
test_save_interaction_to_events_store(item_id=99262)
test_save_interaction_to_events_store(item_id=99263)
test_save_interaction_to_events_store(item_id=101478482)
test_get_interactions_from_events_store()
print("====================\n")


# Тест 3 проверяет получение рекомендаций для пользователя без персональных рекомендаций. Ожидается, что пользователь получит дефолтные рекомендации.
print("===== ТЕСТ 3: Юзер без персонализированных рекомендаций =====")
test_recommendations_no_personal()
print("====================\n")


# Тест 4 проверяет получение рекомендаций для пользователя с персональными рекомендациями, но без онлайн истории.
print("===== ТЕСТ 4: Юзер с персонализированными рекомендациями, но без онлайн истории =====")
test_recommendations_personal_no_online()
print("====================\n")

# Тест 5 проверяет получение рекомендаций для пользователя с персональными рекомендациями и онлайн историей. 
print("===== ТЕСТ 5: Юзер с персонализированными рекомендациями и с онлайн историей =====")
test_combined_recommendations()
print("====================\n")
