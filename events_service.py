# Импортируем необходимые библиотеки
from fastapi import FastAPI

# Создаем класс для хранения событий
class EventStore:
    def __init__(self, max_events_per_user=10):
        self.events = {}
        self.max_events_per_user = max_events_per_user

    def put(self, user_id, item_id):
        user_events = self.events.get(user_id, [])
        self.events[user_id] = [item_id] + user_events[:self.max_events_per_user]

    def get(self, user_id, k):
        user_events = self.events.get(user_id, [])
        return user_events[:k]
    

# Создаем экземпляр хранилища событий
events_store = EventStore()

# Создаем FastAPI приложение
app = FastAPI(title="events")

# Определяем эндпоинт для добавления события
@app.post("/put")
async def put(user_id: int, item_id: int):
    events_store.put(user_id, item_id)
    return {"result": "ok"}

# Определяем эндпоинт для получения событий пользователя
@app.post("/get")
async def get(user_id: int, k: int = 10):
    events = events_store.get(user_id, k)
    return {"events": events}
