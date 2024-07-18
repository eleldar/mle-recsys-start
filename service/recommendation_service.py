import logging

from fastapi import FastAPI
from contextlib import asynccontextmanager
from handler import Recommendations

logger = logging.getLogger("uvicorn.error")
rec_store = Recommendations()


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

@app.post("/recommendations")
async def recommendations(user_id: int, k: int = 100):
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
