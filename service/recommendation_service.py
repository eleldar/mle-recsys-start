import logging
import requests
from fastapi import FastAPI
from contextlib import asynccontextmanager
from handler import Recommendations

logger = logging.getLogger("uvicorn.error")
rec_store = Recommendations()

features_store_url = "http://0.0.0.0:8010"
events_store_url = "http://0.0.0.0:8020"

def dedup_ids(combined: list) -> list:
    unique = []
    for item in combined:
        if item not in unique:
            unique.append(item)
    return unique

@asynccontextmanager
async def lifespan(app: FastAPI):
    # код ниже (до yield) выполнится только один раз при запуске сервиса
    logger.info("Starting")
    rec_store.load(
        "personal",
        "datasets/final_recommendations.parquet", # ваш код здесь #
        columns=["user_id", "item_id", "rank"],
    )
    rec_store.load(
        "default",
        "datasets/top_recs.parquet", # ваш код здесь #,
        columns=["item_id", "rank"],
    )
    yield
    info = rec_store.stats()
    logger.info(info)
    # этот код выполнится только один раз при остановке сервиса
    logger.info("Stopping")
    
# создаём приложение FastAPI
app = FastAPI(title="recommendations", lifespan=lifespan)

@app.post("/recommendations_offline")
async def recommendations_offline(user_id: int, k: int = 100):
    """
    Возвращает список рекомендаций длиной k для пользователя user_id
    """
    recs = rec_store.get(user_id=user_id, k=k)

    return {"recs": recs}

@app.get("/health")
async def health():
    """возвращает статус здоровья"""
    if all(rec_store._recs.get("personal", [])) or all(rec_store._recs.get("default")):
        return "healthy"
    else:
        return "unhealthy"

@app.get("/stats")
async def stats():
    return rec_store.get_stats()

@app.post("/recommendations_online")
async def recommendations_online(user_id: int, k: int = 100):
    """
    Возвращает список онлайн-рекомендаций длиной k для пользователя user_id
    """

    headers = {"Content-type": "application/json", "Accept": "text/plain"}

    # получаем последнее событие пользователя
    params = {"user_id": user_id, "k": 3}
    resp = requests.post(events_store_url + "/get", headers=headers, params=params)
    events = resp.json()
    events = events["events"]

    # получаем список айтемов, похожих на последние три, с которыми взаимодействовал пользователь
    items = []
    scores = []
    for item_id in events:
        # для каждого item_id получаем список похожих в item_similar_items
        # ваш код здесь
        params = {"item_id": item_id, "k": k}
        resp = requests.post(features_store_url +"/similar_items", headers=headers, params=params)
        item_similar_items = resp.json()
        item_similar_items = item_similar_items
        items += item_similar_items["item_id_2"]
        scores += item_similar_items["score"]
    # сортируем похожие объекты по scores в убывающем порядке
    # для старта это приемлемый подход
    combined = list(zip(items, scores))
    combined = sorted(combined, key=lambda x: x[1], reverse=True)
    combined = [item for item, _ in combined]

    # удаляем дубликаты, чтобы не выдавать одинаковые рекомендации
    recs = dedup_ids(combined)[:k]

    return {"recs": recs} 

@app.post("/recommendations")
async def recommendations(user_id: int, k: int = 100):
    """
    Возвращает список рекомендаций длиной k для пользователя user_id
    """

    recs_offline = await recommendations_offline(user_id, k)
    recs_online = await recommendations_online(user_id, k)

    recs_offline = recs_offline["recs"]
    recs_online = recs_online["recs"]

    recs_blended = []

    min_length = min(len(recs_offline), len(recs_online))
    # чередуем элементы из списков, пока позволяет минимальная длина
    for i in range(min_length):
        # ваш код здесь #
        if i % 2 == 0:
            recs_blended.append(recs_offline[i])
        else:
            recs_blended.append(recs_online[i])

    # добавляем оставшиеся элементы в конец
    # ваш код здесь #
    recs_blended = recs_blended + recs_offline[min_length:]
    recs_blended = recs_blended + recs_online[min_length:]

    # удаляем дубликаты
    recs_blended = dedup_ids(recs_blended)
    
    # оставляем только первые k рекомендаций
    # ваш код здесь #
    recs_blended = recs_blended[:k]

    return {"recs": recs_blended}
