"""
Модуль для построения графиков сравнения факта и моделей.
"""

import io
import zipfile
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime

import matplotlib
matplotlib.use('Agg')  # Использовать неинтерактивный бэкенд
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.patches import Rectangle
import numpy as np

from .schemas import WellData, QualityMetrics, SeriesData


# Цветовая схема для факта (синие оттенки)
FACT_COLORS = {
    'data': '#1f77b4',  # Синий для точек данных
    'max': '#2ca02c',    # Зеленый для максимумов
    'min': '#d62728',    # Красный для минимумов
}

# Цветовая схема для моделей (оранжевые/красные оттенки)
MODEL_COLORS = {
    'data': '#ff7f0e',  # Оранжевый для линий данных
    'max': '#9467bd',   # Фиолетовый для максимумов
    'min': '#8c564b',   # Коричневый для минимумов
}


def calculate_common_axes_limits(
    well_data: WellData
) -> Tuple[Tuple[datetime, datetime], Tuple[float, float]]:
    """
    Вычисляет общие пределы осей для всех графиков скважины.
    
    Parameters
    ----------
    well_data : WellData
        Данные скважины
        
    Returns
    -------
    Tuple[Tuple[datetime, datetime], Tuple[float, float]]
        ((min_date, max_date), (min_pressure, max_pressure))
    """
    all_dates: List[datetime] = []
    all_pressures: List[float] = []
    
    # Собираем все даты и давления из факта
    if well_data.fact.dates:
        fact_dates = [datetime.strptime(d, '%Y-%m-%d') for d in well_data.fact.dates]
        all_dates.extend(fact_dates)
        all_pressures.extend(well_data.fact.wbp)
        
        # Добавляем экстремумы
        for ext in well_data.fact.extremums:
            all_dates.append(datetime.strptime(ext.date, '%Y-%m-%d'))
            all_pressures.append(ext.wbp)
    
    # Собираем все даты и давления из моделей
    for model_name, model_data in well_data.models.items():
        if model_data.dates:
            model_dates = [datetime.strptime(d, '%Y-%m-%d') for d in model_data.dates]
            all_dates.extend(model_dates)
            all_pressures.extend(model_data.wbp)
            
            # Добавляем экстремумы
            for ext in model_data.extremums:
                all_dates.append(datetime.strptime(ext.date, '%Y-%m-%d'))
                all_pressures.append(ext.wbp)
    
    if not all_dates or not all_pressures:
        # Возвращаем значения по умолчанию, если данных нет
        default_date = datetime.now()
        return ((default_date, default_date), (0.0, 100.0))
    
    min_date = min(all_dates)
    max_date = max(all_dates)
    
    min_pressure = min(all_pressures)
    max_pressure = max(all_pressures)
    
    # Добавляем небольшой отступ (5% от диапазона)
    date_range = (max_date - min_date).days
    pressure_range = max_pressure - min_pressure
    
    if date_range > 0:
        date_padding = date_range * 0.05
        min_date = datetime.fromordinal(min_date.toordinal() - int(date_padding))
        max_date = datetime.fromordinal(max_date.toordinal() + int(date_padding))
    
    if pressure_range > 0:
        pressure_padding = pressure_range * 0.05
        min_pressure -= pressure_padding
        max_pressure += pressure_padding
    
    return ((min_date, max_date), (min_pressure, max_pressure))


def format_metrics_text(metrics: Optional[QualityMetrics]) -> str:
    """
    Форматирует метрики качества в текст для отображения на графике.
    
    Parameters
    ----------
    metrics : Optional[QualityMetrics]
        Метрики качества
        
    Returns
    -------
    str
        Отформатированный текст метрик
    """
    if not metrics:
        return "Метрики недоступны"
    
    lines = []
    
    if metrics.phase_deviation_days is not None:
        sign = "+" if metrics.phase_deviation_days >= 0 else ""
        lines.append(f"Фаза: {sign}{metrics.phase_deviation_days:.2f} дн.")
    
    if metrics.amplitude_deviation is not None:
        sign = "+" if metrics.amplitude_deviation >= 0 else ""
        lines.append(f"Амплитуда: {sign}{metrics.amplitude_deviation:.2f} бар")
    
    if metrics.max_deviation is not None:
        sign = "+" if metrics.max_deviation >= 0 else ""
        lines.append(f"Максимумы: {sign}{metrics.max_deviation:.2f} бар")
    
    if metrics.min_deviation is not None:
        sign = "+" if metrics.min_deviation >= 0 else ""
        lines.append(f"Минимумы: {sign}{metrics.min_deviation:.2f} бар")
    
    return "\n".join(lines) if lines else "Метрики недоступны"


def create_graph(
    well_name: str,
    model_name: str,
    fact_series: "SeriesData",
    model_series: "SeriesData",
    quality_metrics: Optional[QualityMetrics],
    date_limits: Tuple[datetime, datetime],
    pressure_limits: Tuple[float, float]
) -> io.BytesIO:
    """
    Создает график сравнения факта и модели для одной скважины.
    
    Parameters
    ----------
    well_name : str
        Название скважины
    model_name : str
        Название модели
    fact_series : SeriesData
        Данные факта
    model_series : SeriesData
        Данные модели
    quality_metrics : Optional[QualityMetrics]
        Метрики качества соответствия
    date_limits : Tuple[datetime, datetime]
        Пределы оси X (даты)
    pressure_limits : Tuple[float, float]
        Пределы оси Y (давления)
        
    Returns
    -------
    io.BytesIO
        Буфер с изображением графика в формате PNG
    """
    # Создаем фигуру
    fig, ax = plt.subplots(figsize=(14, 8))
    
    # Парсим даты для факта
    fact_dates = [datetime.strptime(d, '%Y-%m-%d') for d in fact_series.dates]
    fact_pressures = fact_series.wbp
    
    # Парсим даты для модели
    model_dates = [datetime.strptime(d, '%Y-%m-%d') for d in model_series.dates]
    model_pressures = model_series.wbp
    
    # Разделяем экстремумы
    fact_maxima = [ext for ext in fact_series.extremums if ext.type == 'max']
    fact_minima = [ext for ext in fact_series.extremums if ext.type == 'min']
    model_maxima = [ext for ext in model_series.extremums if ext.type == 'max']
    model_minima = [ext for ext in model_series.extremums if ext.type == 'min']
    
    # Построение данных факта (точки)
    if fact_dates and fact_pressures:
        ax.scatter(
            fact_dates,
            fact_pressures,
            color=FACT_COLORS['data'],
            marker='o',
            s=20,
            alpha=0.6,
            label='Факт (давление)',
            zorder=3
        )
    
    # Построение максимумов факта
    if fact_maxima:
        fact_max_dates = [datetime.strptime(ext.date, '%Y-%m-%d') for ext in fact_maxima]
        fact_max_values = [ext.wbp for ext in fact_maxima]
        ax.scatter(
            fact_max_dates,
            fact_max_values,
            color=FACT_COLORS['max'],
            marker='^',
            s=40,
            alpha=0.8,
            label='Факт (максимумы)',
            edgecolors='darkgreen',
            linewidths=1.0,
            zorder=5
        )
    
    # Построение минимумов факта
    if fact_minima:
        fact_min_dates = [datetime.strptime(ext.date, '%Y-%m-%d') for ext in fact_minima]
        fact_min_values = [ext.wbp for ext in fact_minima]
        ax.scatter(
            fact_min_dates,
            fact_min_values,
            color=FACT_COLORS['min'],
            marker='v',
            s=40,
            alpha=0.8,
            label='Факт (минимумы)',
            edgecolors='darkred',
            linewidths=1.0,
            zorder=5
        )
    
    # Построение данных модели (линия)
    if model_dates and model_pressures:
        ax.plot(
            model_dates,
            model_pressures,
            color=MODEL_COLORS['data'],
            linewidth=2,
            alpha=0.8,
            label=f'{model_name} (давление)',
            zorder=2
        )
    
    # Построение максимумов модели
    if model_maxima:
        model_max_dates = [datetime.strptime(ext.date, '%Y-%m-%d') for ext in model_maxima]
        model_max_values = [ext.wbp for ext in model_maxima]
        ax.scatter(
            model_max_dates,
            model_max_values,
            color=MODEL_COLORS['max'],
            marker='^',
            s=40,
            alpha=0.8,
            label=f'{model_name} (максимумы)',
            edgecolors='darkviolet',
            linewidths=1.0,
            zorder=4
        )
    
    # Построение минимумов модели
    if model_minima:
        model_min_dates = [datetime.strptime(ext.date, '%Y-%m-%d') for ext in model_minima]
        model_min_values = [ext.wbp for ext in model_minima]
        ax.scatter(
            model_min_dates,
            model_min_values,
            color=MODEL_COLORS['min'],
            marker='v',
            s=40,
            alpha=0.8,
            label=f'{model_name} (минимумы)',
            edgecolors='saddlebrown',
            linewidths=1.0,
            zorder=4
        )
    
    # Устанавливаем пределы осей
    ax.set_xlim(date_limits)
    ax.set_ylim(pressure_limits)
    
    # Настройка осей
    # Убираем подпись оси X, чтобы не перекрывалась с легендой
    ax.set_ylabel('Пластовое давление, бар', fontsize=12, fontweight='bold')
    
    # Заголовок
    title = f"{model_name}\nСкважина: {well_name}"
    ax.set_title(title, fontsize=14, fontweight='bold', pad=20)
    
    # Настройка сетки
    ax.grid(True, alpha=0.3, linestyle='--', linewidth=0.5)
    ax.set_axisbelow(True)
    
    # Настройка формата дат на оси X
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    
    # Вычисляем количество дней в диапазоне для оптимизации частоты подписей
    date_range_days = (date_limits[1] - date_limits[0]).days
    
    # Адаптивно выбираем интервал подписей в зависимости от диапазона дат
    # чтобы избежать перекрытия, но показывать достаточно часто
    if date_range_days > 2000:
        # Для очень больших диапазонов - реже
        tick_interval_days = 120
        max_ticks = 12
    elif date_range_days > 1000:
        tick_interval_days = 60
        max_ticks = 15
    elif date_range_days > 500:
        tick_interval_days = 30
        max_ticks = 18
    elif date_range_days > 200:
        tick_interval_days = 15
        max_ticks = 20
    else:
        # Для небольших диапазонов - чаще, но не слишком
        tick_interval_days = 7
        max_ticks = 25
    
    # Используем DayLocator с вычисленным интервалом
    ax.xaxis.set_major_locator(mdates.DayLocator(interval=tick_interval_days))
    
    # Ограничиваем максимальное количество подписей, чтобы избежать перекрытия
    # Если тиков слишком много, увеличиваем интервал
    current_ticks = len(ax.xaxis.get_major_ticks())
    if current_ticks > max_ticks:
        # Увеличиваем интервал, чтобы уложиться в лимит
        adjusted_interval = int(date_range_days / max_ticks)
        ax.xaxis.set_major_locator(mdates.DayLocator(interval=adjusted_interval))
    
    plt.setp(ax.xaxis.get_majorticklabels(), rotation=45, ha='right')
    
    # Настройка сетки по датам (вспомогательные линии - без подписей)
    # Используем более частые линии для сетки, но без подписей
    ax.xaxis.set_minor_locator(mdates.DayLocator(interval=max(1, tick_interval_days // 3)))
    ax.grid(True, which='minor', alpha=0.2, linestyle=':', linewidth=0.5)
    
    # Создаем легенду с двумя столбцами: факты слева, модели справа
    # Собираем все handles и labels в правильном порядке для двух столбцов
    all_handles = []
    all_labels = []
    
    # Первый столбец: факты (3 ряда)
    if fact_dates and fact_pressures:
        all_handles.append(plt.Line2D([0], [0], marker='o', color='w', 
                                     markerfacecolor=FACT_COLORS['data'], 
                                     markersize=8, alpha=0.6))
        all_labels.append('Факт (давление)')
    else:
        all_handles.append(plt.Line2D([0], [0], visible=False))
        all_labels.append('')
    
    if fact_maxima:
        all_handles.append(plt.Line2D([0], [0], marker='^', color='w', 
                                     markerfacecolor=FACT_COLORS['max'], 
                                     markeredgecolor='darkgreen',
                                     markersize=10, markeredgewidth=1.0))
        all_labels.append('Факт (максимумы)')
    else:
        all_handles.append(plt.Line2D([0], [0], visible=False))
        all_labels.append('')
    
    if fact_minima:
        all_handles.append(plt.Line2D([0], [0], marker='v', color='w', 
                                     markerfacecolor=FACT_COLORS['min'], 
                                     markeredgecolor='darkred',
                                     markersize=10, markeredgewidth=1.0))
        all_labels.append('Факт (минимумы)')
    else:
        all_handles.append(plt.Line2D([0], [0], visible=False))
        all_labels.append('')
    
    # Второй столбец: модель (3 ряда)
    if model_dates and model_pressures:
        all_handles.append(plt.Line2D([0], [0], color=MODEL_COLORS['data'], 
                                     linewidth=2, alpha=0.8))
        all_labels.append(f'{model_name} (давление)')
    else:
        all_handles.append(plt.Line2D([0], [0], visible=False))
        all_labels.append('')
    
    if model_maxima:
        all_handles.append(plt.Line2D([0], [0], marker='^', color='w', 
                                     markerfacecolor=MODEL_COLORS['max'], 
                                     markeredgecolor='darkviolet',
                                     markersize=10, markeredgewidth=1.0))
        all_labels.append(f'{model_name} (максимумы)')
    else:
        all_handles.append(plt.Line2D([0], [0], visible=False))
        all_labels.append('')
    
    if model_minima:
        all_handles.append(plt.Line2D([0], [0], marker='v', color='w', 
                                     markerfacecolor=MODEL_COLORS['min'], 
                                     markeredgecolor='saddlebrown',
                                     markersize=10, markeredgewidth=1.0))
        all_labels.append(f'{model_name} (минимумы)')
    else:
        all_handles.append(plt.Line2D([0], [0], visible=False))
        all_labels.append('')
    
    # Создаем легенду с двумя столбцами (ncol=2) и тремя рядами
    legend = ax.legend(
        all_handles, all_labels,
        loc='upper center',
        bbox_to_anchor=(0.5, -0.15),
        ncol=2,  # Два столбца
        fontsize=9,
        frameon=False,  # Убираем рамку
        facecolor='white',  # Белый фон
        edgecolor='none',
        columnspacing=1.5,  # Расстояние между столбцами
        handletextpad=0.5  # Расстояние между маркером и текстом
    )
    
    # Добавляем метрики качества в текстовое поле
    if quality_metrics:
        metrics_text = format_metrics_text(quality_metrics)
        
        # Создаем текстовое поле с метриками
        textstr = f"Метрики качества:\n{metrics_text}"
        
        # Размещаем в правом нижнем углу
        props = dict(boxstyle='round', facecolor='wheat', alpha=0.8)
        ax.text(
            0.98,
            0.02,
            textstr,
            transform=ax.transAxes,
            fontsize=10,
            verticalalignment='bottom',
            horizontalalignment='right',
            bbox=props
        )
    
    # Улучшаем компоновку с учетом легенды под графиком
    plt.tight_layout(rect=[0, 0.05, 1, 0.98])
    
    # Сохраняем в буфер
    buf = io.BytesIO()
    plt.savefig(buf, format='png', dpi=150, bbox_inches='tight')
    buf.seek(0)
    plt.close(fig)
    
    return buf


def generate_graphs_archive(wells_data: List[WellData]) -> io.BytesIO:
    """
    Генерирует архив с графиками для всех скважин и моделей.
    
    Parameters
    ----------
    wells_data : List[WellData]
        Список данных по скважинам
        
    Returns
    -------
    io.BytesIO
        Буфер с ZIP-архивом, содержащим все графики
    """
    zip_buffer = io.BytesIO()
    
    with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
        graphs_count = 0
        
        for well_data in wells_data:
            # Вычисляем общие пределы осей для всех графиков этой скважины
            date_limits, pressure_limits = calculate_common_axes_limits(well_data)
            
            # Создаем график для каждой модели
            for model_name, model_series in well_data.models.items():
                # Получаем метрики качества для этой модели
                # Pydantic уже преобразовал словарь в объект QualityMetrics
                quality_metrics = well_data.quality_metrics.get(model_name)
                
                # Создаем график
                graph_buffer = create_graph(
                    well_name=well_data.well_name,
                    model_name=model_name,
                    fact_series=well_data.fact,
                    model_series=model_series,
                    quality_metrics=quality_metrics,
                    date_limits=date_limits,
                    pressure_limits=pressure_limits
                )
                
                # Формируем имя файла (безопасное для файловой системы)
                safe_well_name = well_data.well_name.replace('/', '_').replace('\\', '_')
                safe_model_name = model_name.replace('/', '_').replace('\\', '_')
                filename = f"{safe_well_name}_{safe_model_name}.png"
                
                # Добавляем в архив
                zip_file.writestr(filename, graph_buffer.read())
                graphs_count += 1
        
        # Добавляем файл с информацией об архиве
        info_text = f"Архив графиков\nСоздан: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\nКоличество графиков: {graphs_count}"
        zip_file.writestr("INFO.txt", info_text)
    
    zip_buffer.seek(0)
    return zip_buffer

