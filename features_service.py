# Импортируем необходимые библиотеки
import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI
from s3_utils import load_parquet_from_s3

# Инициализируем логгер
logger = logging.getLogger("uvicorn.error")

# Создаем класс для хранения похожих товаров
class SimilarItems:
    def __init__(self):
        self._similar_items = None

    # Метод для загрузки данных о похожих товарах из S3
    def load(self, key, **kwargs):
        logger.info(f"Загружаем данные из S3: {key}")
        self._similar_items = load_parquet_from_s3(key, **kwargs)
        self._similar_items = self._similar_items.set_index("item_id_1")
        logger.info(f"Загружено")

    # Метод для получения похожих товаров для заданного item_id
    def get(self, item_id: int, k: int = 100):
        try:
            i2i = self._similar_items.loc[item_id].head(k)
            i2i = i2i[["item_id_2", "score"]].to_dict(orient="list")
        except KeyError:
            logger.error("Результаты не найдены")
            i2i = {"item_id_2": [], "score": {}}
        return i2i
    
# Создаем экземпляр хранилища похожих товаров
sim_items_store = SimilarItems()

# Определяем функцию для управления жизненным циклом приложения FastAPI
# В этой функции мы загружаем данные о похожих товарах при запуске приложения и выводим сообщение о готовности
@asynccontextmanager
async def lifespan(app: FastAPI):
    sim_items_store.load(
        key="similar.parquet",
        columns=["item_id_1", "item_id_2", "score"],
    )
    logger.info("Готово!")
    yield


# Создаем FastAPI приложение и указываем функцию для управления жизненным циклом
app = FastAPI(title="features", lifespan=lifespan)

# Определяем эндпоинт для получения похожих товаров
@app.post("/similar_items")
async def recommendations(item_id: int, k: int = 10):
    i2i = sim_items_store.get(item_id, k)
    return i2i
