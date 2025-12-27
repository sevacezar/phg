"""
Главный файл для запуска FastAPI сервера построения графиков.
"""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from .router import router

app = FastAPI(
    title="Graph Maker API",
    description="API для построения графиков сравнения факта и моделей",
    version="1.0.0"
)

# Настройка CORS (разрешаем все источники для разработки)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Подключаем роутер
app.include_router(router)


@app.get("/")
async def root():
    """
    Корневой эндпойнт.
    
    Returns
    -------
    dict
        Информация о сервисе
    """
    return {
        "service": "Graph Maker API",
        "version": "1.0.0",
        "endpoints": {
            "POST /api/generate_graphs": "Генерация графиков",
            "GET /api/health": "Проверка работоспособности"
        }
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

