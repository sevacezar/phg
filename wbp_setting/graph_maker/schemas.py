"""
Схемы данных для API построения графиков.
"""

from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field


class ExtremumPoint(BaseModel):
    """Точка экстремума (максимум или минимум)."""
    date: str = Field(..., description="Дата экстремума в формате YYYY-MM-DD")
    wbp: float = Field(..., description="Значение давления в барах")
    type: str = Field(..., description="Тип экстремума: 'max' или 'min'")


class SeriesData(BaseModel):
    """Данные временного ряда (факт или модель)."""
    dates: List[str] = Field(..., description="Список дат в формате YYYY-MM-DD")
    wbp: List[float] = Field(..., description="Список значений давления в барах")
    extremums: List[ExtremumPoint] = Field(default_factory=list, description="Список экстремумов")


class QualityMetrics(BaseModel):
    """Метрики качества соответствия модели факту."""
    phase_deviation_days: Optional[float] = Field(None, description="Отклонение по фазе в днях")
    amplitude_deviation: Optional[float] = Field(None, description="Отклонение амплитуды в барах")
    max_deviation: Optional[float] = Field(None, description="Отклонение максимумов в барах")
    min_deviation: Optional[float] = Field(None, description="Отклонение минимумов в барах")


class WellData(BaseModel):
    """Данные для одной скважины."""
    well_name: str = Field(..., description="Название скважины")
    fact: SeriesData = Field(..., description="Фактические данные")
    models: Dict[str, SeriesData] = Field(default_factory=dict, description="Данные моделей (ключ - название модели)")
    quality_metrics: Dict[str, QualityMetrics] = Field(
        default_factory=dict,
        description="Метрики качества для каждой модели (ключ - название модели)"
    )


class GraphRequest(BaseModel):
    """Запрос на построение графиков."""
    wells: List[WellData] = Field(..., description="Список данных по скважинам")


class GraphResponse(BaseModel):
    """Ответ с архивом графиков."""
    message: str = Field(..., description="Сообщение о результате")
    archive_size: int = Field(..., description="Размер архива в байтах")
    graphs_count: int = Field(..., description="Количество построенных графиков")

