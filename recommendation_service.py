# Импортируем необходимые библиотеки
import logging
from fastapi import FastAPI
from contextlib import asynccontextmanager
import requests
from s3_utils import load_parquet_from_s3

# Инициализируем логгер
logger = logging.getLogger("uvicorn.error")

# Указываем URL для доступа к сервисам фичей и событий
feature_store_url = "http://0.0.0.0:8000"
events_store_url = "http://0.0.0.0:8001"

# Создаем класс для хранения рекомендаций
class Recommendations:
    # Инициализируем словарь для хранения персональных и дефолтных рекомендаций, а также статистику по запросам
    def __init__(self):
        self._recs = {"personal": None, "default": None}
        self._stats = {
            "request_personal_count": 0,
            "request_default_count": 0
        }

    # Метод для загрузки рекомендаций из S3. В зависимости от типа рекомендаций (персональные или дефолтные) данные сохраняются в соответствующем ключе.
    def load(self, type, key, **kwargs):
        logger.info(f"Loading recommendations, type: {type}")
        self._recs[type] = load_parquet_from_s3(key, **kwargs)
        if type == "personal":
            self._recs[type] = self._recs[type].set_index("user_id")
        logger.info(f"Загружено")
    
    # Метод для получения рекомендаций для заданного user_id.
    def get(self, user_id: int, k: int = 100):
        try:
            recs = self._recs["personal"].loc[user_id]
            recs = recs["item_id"].to_list()[:k]
            self._stats["request_personal_count"] += 1
        except KeyError:
            recs = self._recs["default"]
            recs = recs["item_id"].to_list()[:k]
            self._stats["request_default_count"] += 1
        except:
            logger.error("Рекоммендации не найдены")
            recs = []
        return recs
    
    # Метод для вывода статистики по запросам рекомендаций. Выводит количество запросов персональных и дефолтных рекомендаций.
    def stats(self):
        logger.info("Статистика по запросам рекомендаций:")
        for name, value in self._stats.items():
            logger.info(f"{name:<30} {value}")


# Создаем экземпляр хранилища рекомендаций
rec_store = Recommendations()

# Функция для удаления дубликатов из списка идентификаторов.
def dedup_ids(ids):
    seen = set()
    ids = [id for id in ids if not (id in seen or seen.add(id))]
    return ids


# Определяем функцию для управления жизненным циклом приложения FastAPI. Загружаем персональные и дефолтные рекомендации при запуске приложения и выводит статистику.
@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Начинаем загрузку рекомендаций")
    rec_store.load(
        type="personal",
        key="recommendations.parquet",
        columns=["user_id", "item_id", "rank"]
    )

    rec_store.load(
        type="default",
        key="top_popular.parquet",
        columns=["item_id", "popularity_score"]
    )
    yield
    logger.info("Статистика по запросам рекомендаций:")
    rec_store.stats()
    logger.info("Остановлено")


# Создаем FastAPI приложение и указываем функцию для управления жизненным циклом
app = FastAPI(title="recommendations", lifespan=lifespan)

# Определяем эндпоинт для получения оффлайн рекомендаций. Получаем рекомендации из хранилища рекомендаций и возвращаем их в виде JSON.
@app.post("/recommendations_offline")
async def recommendations_offline(user_id: int, k: int = 100):
    recs = rec_store.get(user_id=user_id, k=k)
    return {"recs": recs}

# Определяем эндпоинт для получения онлайн рекомендаций.
@app.post("/recommendations_online")
async def recommendations_online(user_id: int, k: int = 100):
    headers = {"Content-Type": "application/json", "Accept": "text/plain"}
    params = {"user_id": user_id, "k": k}
    # Получаем последние взаимодействия пользователя из сервиса событий.
    resp = requests.post(events_store_url + "/get", headers=headers, params=params)
    events = resp.json()
    events = events["events"]

    items = []
    scores = []

    # Для каждого взаимодействия получаем похожие товары из сервиса фичей и сохраняем их вместе с оценками в списки.
    for item_id in events:
        params = {"item_id": item_id, "k": k}
        resp = requests.post(feature_store_url + "/similar_items", headers=headers, params=params)
        item_similar = resp.json()
        item_similar_items = item_similar.get("item_id_2", [])
        item_similar_scores = item_similar.get("score", [])
        items += item_similar_items
        scores += item_similar_scores

    # Объединяем товары и их оценки в кортежи, сортируем их по оценкам в порядке убывания и извлекаем отсортированные товары.
    combined = list(zip(items, scores))
    combined = sorted(combined, key=lambda x: x[1], reverse=True)
    combined = [item for item, _ in combined]

    # Удаляем дубликаты из списка товаров и возвращаем топ-k товаров в виде JSON.
    recs = dedup_ids(combined)
    recs = recs[:k]
    return {"recs": recs}


# Определяем эндпоинт для получения комбинированных рекомендаций.
@app.post("/recommendations")
async def recommendations(user_id: int, k: int = 100):
    # Получаем оффлайн и онлайн рекомендации для пользователя
    recs_offline = await recommendations_offline(user_id, k)
    recs_online = await recommendations_online(user_id, k)

    # Извлекаем списки рекомендаций из ответов оффлайн и онлайн эндпоинтов.
    recs_offline = recs_offline["recs"]
    recs_online = recs_online["recs"]

    recs_blended = []

    # Определяем минимальную длину между оффлайн и онлайн рекомендациями
    min_length = min(len(recs_offline), len(recs_online))

    # Чередуем оффлайн и онлайн рекомендации до минимальной длины, а затем добавляем оставшиеся рекомендации из обоих списков.
    for i in range(min_length):
        recs_blended.append(recs_online[i])
        recs_blended.append(recs_offline[i])

    recs_blended += recs_online[min_length:]
    recs_blended += recs_offline[min_length:]

    # Удаляем дубликаты из объединенного списка рекомендаций и возвращаем топ-k товаров в виде JSON.
    recs_blended = dedup_ids(recs_blended)
    recs_blended = recs_blended[:k]

    return {"recs": recs_blended}
