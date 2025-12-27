"""
FastAPI роутер для построения графиков.
"""
import os
from typing import List
from fastapi import APIRouter, HTTPException, Response
from fastapi.responses import StreamingResponse

from .schemas import GraphRequest, GraphResponse
from .graph_maker import generate_graphs_archive

SERVICE_NAME: str = os.path.basename(os.path.dirname(__file__))


router = APIRouter(prefix="/adapt_wbp_phg", tags=[SERVICE_NAME])


@router.post("", response_model=GraphResponse)
async def generate_graphs(request: GraphRequest) -> StreamingResponse:
    """
    Генерирует графики сравнения факта и моделей для всех скважин.
    
    Parameters
    ----------
    request : GraphRequest
        Запрос с данными для построения графиков
        
    Returns
    -------
    StreamingResponse
        ZIP-архив с графиками в формате PNG
        
    Raises
    ------
    HTTPException
        Если произошла ошибка при генерации графиков
    """
    try:
        # Данные уже в формате WellData благодаря Pydantic
        wells_data = request.wells
        
        # Фильтруем скважины без данных
        wells_data = [
            well_data for well_data in wells_data
            if well_data.fact.dates and well_data.models
        ]
        
        if not wells_data:
            raise HTTPException(
                status_code=400,
                detail="Нет данных для построения графиков. Убедитесь, что в запросе есть данные факта и моделей."
            )
        
        # Генерируем архив с графиками
        archive_buffer = generate_graphs_archive(wells_data)
        
        # Получаем размер архива
        archive_size = len(archive_buffer.getvalue())
        
        # Подсчитываем количество графиков
        graphs_count = sum(
            len(well_data.models) for well_data in wells_data
        )
        
        # Создаем ответ с архивом
        return StreamingResponse(
            iter([archive_buffer.getvalue()]),
            media_type="application/zip",
            headers={
                "Content-Disposition": "attachment; filename=graphs_archive.zip",
                "Content-Length": str(archive_size)
            }
        )
        
    except HTTPException:
        raise
    except Exception as e:
        print(str(e))
        raise HTTPException(
            status_code=500,
            detail=f"Ошибка при генерации графиков: {str(e)}"
        )
