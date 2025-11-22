#!/usr/bin/env python3
"""
ETL скрипт для импорта данных из Excel файлов 1С в БД

Поддерживаемые типы файлов:
1. Отливка.xlsx - потребности в деталях (detail_requirements)
2. Остатки.xlsx - инвентарь склада (inventory_snapshots)
3. Металл.xlsx - остатки металла (material_inventory_snapshots)

Использование:
    python etl_1c.py --connection "postgresql://..." --requirements отливка.xlsx
    python etl_1c.py --connection "postgresql://..." --inventory остатки.xlsx
    python etl_1c.py --connection "postgresql://..." --materials металл.xlsx
"""

import argparse
import sys
import os
from pathlib import Path
from datetime import datetime, date
import re
import pandas as pd
import numpy as np
import psycopg2
from psycopg2.extras import execute_batch

# ============================================================================
# ПАРСЕРЫ ФАЙЛОВ 1С
# ============================================================================

# Константы
PHASES = ['Отливка', 'Зачистка', 'Дробеструй', 'Токарка', 'Фрезеровка', 'Слесарка']

def is_empty_row(row):
    """Проверка что строка пустая"""
    return row.isna().all() or (row.astype(str).str.strip() == '').all()


def parse_hierarchical_file(filepath, level_matchers_builder, record_builder):
    """
    Универсальный парсер иерархических файлов из 1С
    
    Args:
        filepath: путь к файлу
        level_matchers_builder: функция(hierarchy_levels) -> list[matcher_func]
        record_builder: функция(current_level, level_name, cell_value, row, state, data_columns) -> record или None
    
    Возвращает: список записей
    """
    df = pd.read_excel(filepath, sheet_name=0, header=None)
    nrows, ncols = df.shape
    
    # 1. Пропускаем служебные строки (содержат ':')
    current_row = 0
    
    while current_row < min(15, nrows):
        row = df.iloc[current_row]
        if is_empty_row(row):
            current_row += 1
            continue
        
        first_cell = None
        for col in range(ncols):
            val = str(row[col]) if pd.notna(row[col]) else ''
            if val.strip():
                first_cell = val
                break
        
        if first_cell and ':' in first_cell:
            print(f"⏭️  Пропуск служебной строки {current_row}: {first_cell[:50]}...")
            current_row += 1
            continue
        
        # Заголовки найдены?
        if first_cell and re.search(r'Характеристика|Номенклатура|Склад', first_cell, re.IGNORECASE):
            break
        
        current_row += 1
    
    # 2. Парсим заголовки
    hierarchy_levels = []
    data_columns = [''] * ncols
    header_row = current_row
    
    if header_row < nrows:
        print(f"\n📋 Чтение заголовков начиная со строки {header_row}")
        
        level_idx = 0
        while header_row < nrows:
            row = df.iloc[header_row]
            
            if is_empty_row(row):
                break
            
            # Вертикально: иерархия
            hierarchy_cell_col = None
            for col in range(ncols):
                val = str(row[col]) if pd.notna(row[col]) else ''
                val = val.strip()
                if val and val != '-':
                    hierarchy_levels.append({
                        'col': col,
                        'name': val
                    })
                    hierarchy_cell_col = col
                    print(f"   Уровень {level_idx}: колонка {col} - '{val}'")
                    level_idx += 1
                    break
            
            # Горизонтально: data_columns
            for col in range(ncols):
                if col == hierarchy_cell_col:
                    continue
                val = str(row[col]) if pd.notna(row[col]) else ''
                val = val.strip()
                if val and val != '-':
                    data_columns[col] = val
            
            header_row += 1
        
        print(f"\n📊 Колонки данных:")
        for col_idx, col_name in enumerate(data_columns):
            if col_name:
                print(f"   Колонка {col_idx}: '{col_name}'")
    
    if not hierarchy_levels:
        print("❌ Не найдены заголовки иерархии")
        return []
    
    # 3. Начало данных
    start_row = header_row
    while start_row < nrows and is_empty_row(df.iloc[start_row]):
        start_row += 1
    
    print(f"\n📊 Начало данных: строка {start_row}\n")
    
    # 4. Строим матчеры
    level_matchers = level_matchers_builder(hierarchy_levels)
    print(f"📊 Матчеры уровней: {len(level_matchers)} уровней\n")
    
    # 5. Парсим данные
    records = []
    state = {}
    hierarchy_col = hierarchy_levels[0]['col'] if hierarchy_levels else 0
    current_level = 0
    
    for i in range(start_row, nrows):
        row = df.iloc[i]
        if is_empty_row(row):
            continue
        
        cell_value = row[hierarchy_col]
        if pd.isna(cell_value) or not str(cell_value).strip() or str(cell_value).strip() == '-':
            continue
        
        cell_value = str(cell_value).strip()
        
        # Пробуем матчить
        matched = False
        for level_idx, matcher in enumerate(level_matchers):
            if matcher(cell_value):
                current_level = level_idx
                matched = True
                break
        
        # Не совпало - инкремент или сброс
        if not matched:
            if current_level >= len(level_matchers) - 1:
                current_level = 0
            else:
                current_level += 1
        
        print(f"Строка {i:3d} | Уровень {current_level}: {cell_value[:50]}")
        
        # Обработка через callback
        level_name = hierarchy_levels[current_level]['name'].lower() if current_level < len(hierarchy_levels) else ''
        record = record_builder(current_level, level_name, cell_value, row, state, data_columns)
        
        if record:
            records.append(record)
    
    return records


def parse_inventory_file(filepath, snapshot_date=None):
    """Парсинг файла "Товары на складах" """
    if snapshot_date is None:
        snapshot_date = datetime.now().date()
    
    # Строим матчеры для инвентаря
    def build_matchers(hierarchy_levels):
        def is_nomenclature(text):
            if text.startswith('Алюминий') and 'сплав' in text.lower():
                return True
            if re.search(r'К\d+\.\d+\.\d+', text):
                return True
            return False
        
        def is_characteristic(text):
            if any(text.startswith(p) for p in PHASES):
                return True
            if text.startswith('Алюминий') and ('месяц' in text.lower() or 'месац' in text.lower()):
                return True
            return False
        
        def is_warehouse(text):
            warehouse_keywords = ['цех', 'бокс', 'этаж', 'Склад', 'Малярка', 
                                 'Материалы', 'Брак', 'шоссе']
            return any(kw in text for kw in warehouse_keywords)
        
        matchers = []
        for level in hierarchy_levels:
            name = level['name'].lower()
            if 'номенклатура' in name:
                matchers.append(is_nomenclature)
            elif 'характеристика' in name:
                matchers.append(is_characteristic)
            elif 'склад' in name:
                matchers.append(is_warehouse)
            else:
                matchers.append(lambda x: False)
        return matchers
    
    # Обработчик записей
    inventory_state = {
        'nomenclature': None,
        'detail_code': None,
        'characteristic': None,
        'warehouse': None
    }
    
    # Ищем колонку "Конечный остаток"
    quantity_col_cache = [None]
    
    def build_record(current_level, level_name, cell_value, row, state, data_columns):
        # Инициализируем state
        if 'detail_code' not in state:
            state.update(inventory_state)
        
        # Кэшируем колонку количества
        if quantity_col_cache[0] is None:
            for col_idx, col_name in enumerate(data_columns):
                if col_name and ('Конечный' in col_name or 'конечный' in col_name.lower()):
                    quantity_col_cache[0] = col_idx
                    break
        
        # Уровень 0: Номенклатура
        if 'номенклатура' in level_name:
            state['nomenclature'] = cell_value
            match = re.search(r'К\d+\.\d+\.\d+[\.\d]*', cell_value)
            if match:
                state['detail_code'] = match.group(0)
            else:
                state['detail_code'] = None
            state['characteristic'] = None
            state['warehouse'] = None
        
        # Уровень 1: Характеристика
        elif 'характеристика' in level_name:
            state['characteristic'] = cell_value
            state['warehouse'] = None
        
        # Уровень 2: Склад
        elif 'склад' in level_name:
            state['warehouse'] = cell_value.strip()
            
            if state['detail_code']:
                quantity = 0
                if quantity_col_cache[0] is not None:
                    val = row[quantity_col_cache[0]]
                    if pd.notna(val) and val != '-':
                        try:
                            quantity = int(float(str(val).replace(',', '.').replace(' ', '')))
                        except:
                            pass
                
                return {
                    'detail_code': state['detail_code'],
                    'characteristic': state['characteristic'],
                    'warehouse': state['warehouse'],
                    'snapshot_date': snapshot_date,
                    'quantity': quantity
                }
        
        return None
    
    return parse_hierarchical_file(filepath, build_matchers, build_record)


def parse_requirements_file(filepath, phase_filter=None):
    """
    Парсинг файла "Анализ обеспеченности заказов" (Отливка.xlsx)
    
    Args:
        filepath: путь к файлу
        phase_filter: фильтр по фазе ('ot'|'za'|'dr'|'fr'|'ma'|'all'|None)
    """
    phase_map = {
        'ot': 'отливка',
        'za': 'зачистка', 
        'dr': 'дробеструй',
        'fr': 'фрезеровка',
        'ma': 'материал'
    }
    
    df = pd.read_excel(filepath, sheet_name=0, header=None)
    nrows, ncols = df.shape
    
    # 1. Пропускаем служебные строки (содержат двоеточие)
    current_row = 0
    
    while current_row < min(15, nrows):
        row = df.iloc[current_row]
        if is_empty_row(row):
            current_row += 1
            continue
        
        # Первая непустая ячейка
        first_cell = None
        for col in range(ncols):
            val = str(row[col]) if pd.notna(row[col]) else ''
            if val.strip():
                first_cell = val
                break
        
        # Служебная строка = содержит двоеточие
        if first_cell and ':' in first_cell:
            print(f"⏭️  Пропуск служебной строки {current_row}: {first_cell[:50]}...")
            current_row += 1
            continue
        
        # Заголовки найдены?
        if first_cell and re.search(r'Характеристика|Номенклатура|Заказ', first_cell):
            break
        
        current_row += 1
    
    # 2. Парсим заголовки - иерархия (вертикально) и колонки данных (горизонтально)
    hierarchy_levels = []
    data_columns = [''] * ncols  # Массив названий колонок данных
    header_row = current_row
    
    if header_row < nrows:
        print(f"\n📋 Чтение заголовков начиная со строки {header_row}")
        
        # Читаем все строки заголовков до первой пустой
        level_idx = 0
        while header_row < nrows:
            row = df.iloc[header_row]
            
            # Пустая строка = конец заголовков
            if is_empty_row(row):
                break
            
            # Вертикально: ищем первую непустую ячейку для иерархии
            hierarchy_cell_col = None
            for col in range(ncols):
                val = str(row[col]) if pd.notna(row[col]) else ''
                val = val.strip()
                if val and val != '-':
                    hierarchy_levels.append({
                        'col': col,
                        'name': val
                    })
                    hierarchy_cell_col = col
                    print(f"   Уровень {level_idx}: колонка {col} - '{val}'")
                    level_idx += 1
                    break
            
            # Горизонтально: остальные ячейки в data_columns (пропускаем иерархию)
            for col in range(ncols):
                if col == hierarchy_cell_col:
                    continue  # Пропускаем колонку иерархии
                val = str(row[col]) if pd.notna(row[col]) else ''
                val = val.strip()
                if val and val != '-':
                    data_columns[col] = val  # Перезаписываем (для merged cells)
            
            header_row += 1
        
        # Выводим data_columns
        print(f"\n📊 Колонки данных:")
        for col_idx, col_name in enumerate(data_columns):
            if col_name:
                print(f"   Колонка {col_idx}: '{col_name}'")
    
    if not hierarchy_levels:
        print("❌ Не найдены заголовки иерархии")
        return []
    
    # 3. Начало данных - после заголовков (header_row уже указывает на пустую строку или первую строку данных)
    start_row = header_row
    while start_row < nrows and is_empty_row(df.iloc[start_row]):
        start_row += 1
    
    print(f"\n📊 Начало данных: строка {start_row}\n")
    
    # 4. Парсим данные: паттерны для уровней + автоинкремент
    records = []
    state = {'phase': None, 'assembly': None, 'detail_code': None}
    
    # Определяем колонку иерархии (первый уровень)
    hierarchy_col = hierarchy_levels[0]['col'] if hierarchy_levels else 1
    
    # Ищем колонку "Потребность"
    quantity_col = None
    for col_idx, col_name in enumerate(data_columns):
        if 'Потребность' in col_name:
            quantity_col = col_idx
            break
    
    print(f"\n📊 Колонка иерархии: {hierarchy_col}, Колонка количества: {quantity_col}\n")
    
    # Определяем паттерны для каждого уровня иерархии
    def is_phase(text):
        if any(text.startswith(p) for p in PHASES):
            return True
        
        # Алюминий как фаза
        if text.startswith('Алюминий') and 'мес' in text.lower():
            return True
        
        return False
    
    def is_assembly(text):
        return bool(re.search(r'^\d{4}$|кресло|Лестница|Комплект|Опора|Привод|Поручень', text))
    
    def is_okp(text):
        return bool(re.match(r'^\(\d+-\d+\)$', text))  # (1-4)
    
    def is_detail(text):
        # Номенклатура алюминий: начинается с "Алюминий" и содержит "сплав"
        if text.startswith('Алюминий') and 'сплав' in text.lower():
            return True
        
        # Детали с кодом К##.##.###
        return bool(re.search(r'К\d+\.\d+\.\d+', text))
    
    def is_date(text):
        return bool(re.search(r'\d{2}\.\d{2}\.\d{4}', text))
    
    # Динамически строим level_matchers из hierarchy_levels
    level_matchers = []
    for level in hierarchy_levels:
        name = level['name'].lower()
        if 'характеристика' in name and 'наименование' in name:
            level_matchers.append(is_phase)
        elif 'артикул' in name:
            level_matchers.append(is_assembly)
        elif 'окп' in name:
            level_matchers.append(is_okp)
        elif 'номенклатура' in name:
            level_matchers.append(is_detail)
        elif 'дата' in name:
            level_matchers.append(is_date)
        else:
            # Неизвестный уровень - пропускаем
            level_matchers.append(lambda x: False)
    
    print(f"📊 Матчеры уровней: {len(level_matchers)} уровней\n")
    
    current_level = 0
    
    for i in range(start_row, nrows):
        row = df.iloc[i]
        if is_empty_row(row):
            continue
        
        # Читаем из фиксированной колонки иерархии
        cell_value = row[hierarchy_col]
        if pd.isna(cell_value) or not str(cell_value).strip() or str(cell_value).strip() == '-':
            continue
        
        cell_value = str(cell_value).strip()
        
        # Пробуем матчить против всех уровней
        matched = False
        for level_idx, matcher in enumerate(level_matchers):
            if matcher(cell_value):
                current_level = level_idx
                matched = True
                break
        
        # Если не совпало - инкремент или сброс
        if not matched:
            if current_level >= len(level_matchers) - 1:
                current_level = 0  # Сброс
            else:
                current_level += 1
        
        print(f"Строка {i:3d} | Уровень {current_level}: {cell_value[:50]}")
        
        # Обработка по типу уровня (не по номеру!)
        level_name = hierarchy_levels[current_level]['name'].lower() if current_level < len(hierarchy_levels) else ''
        
        # Фаза
        if 'характеристика' in level_name and 'наименование' in level_name:
            phase = cell_value.split()[0].lower()
            if phase == 'алюминий': phase = 'материал'
            elif phase == 'токарка': phase = 'фрезеровка'
            state['phase'] = phase
            state['assembly'] = None
            state['detail_code'] = None
        
        # Сборка/Артикул
        elif 'артикул' in level_name:
            state['assembly'] = cell_value
            state['detail_code'] = None
        
        # ОКП - пропускаем
        elif 'окп' in level_name:
            pass
        
        # Деталь (Номенклатура)
        elif 'номенклатура' in level_name and 'артикул' not in level_name:
            match = re.search(r'\((К\d+\.\d+\.\d+[^\)]*)\)', cell_value)
            if match:
                state['detail_code'] = match.group(1)
            else:
                match = re.search(r'(К\d+\.\d+\.\d+[\.\d]*)', cell_value)
                if match:
                    state['detail_code'] = match.group(0)
        
        # Дата
        elif 'дата' in level_name:
            if state['detail_code'] and state['phase']:
                try:
                    req_date = datetime.strptime(cell_value.split()[0], '%d.%m.%Y').date()
                    req_month = req_date.replace(day=1)
                    
                    # Количество из колонки "Потребность"
                    quantity = 0
                    if quantity_col is not None:
                        val = row[quantity_col]
                        if pd.notna(val) and val != '-':
                            try:
                                quantity = int(float(str(val).replace(',', '.')))
                            except:
                                pass
                    
                    if quantity > 0:
                        record = {
                            'detail_code': state['detail_code'],
                            'phase': state['phase'],
                            'assembly': state['assembly'],
                            'requirement_month': req_month,
                            'required_quantity': quantity
                        }
                        
                        if phase_filter is None or phase_filter == 'all':
                            records.append(record)
                        elif phase_filter in phase_map and state['phase'] == phase_map[phase_filter]:
                            records.append(record)
                except (ValueError, AttributeError):
                    pass
    
    return records

def parse_materials_file(filepath):
    """
    Парсинг файла остатков металла
    
    Ожидаемая структура:
    - Колонки: Материал | Количество(кг)
    
    Возвращает: список dict с полями:
        - material_type: тип материала
        - quantity_kg: количество в кг
    """
    df = pd.read_excel(filepath, sheet_name=0, header=None)
    
    # Ищем заголовки
    header_row = None
    for i in range(min(20, len(df))):
        row_str = ' '.join([str(x) for x in df.iloc[i].tolist() if pd.notna(x)])
        if 'Материал' in row_str or 'материал' in row_str.lower():
            header_row = i
            break
    
    if header_row is None:
        raise ValueError("Не найдена строка с заголовками (должна содержать 'Материал')")
    
    df = pd.read_excel(filepath, sheet_name=0, header=header_row)
    
    records = []
    for _, row in df.iterrows():
        if pd.isna(row.get('Материал')):
            continue
        
        material = str(row.get('Материал', '')).strip()
        quantity = row.get('Количество', 0)
        
        # Конвертируем в кг если нужно
        if 'г' in str(row.get('Единица', '')).lower():
            quantity = quantity / 1000
        
        if material and quantity > 0:
            records.append({
                'material_type': material,
                'quantity_kg': float(quantity)
            })
    
    return records

# ============================================================================
# ЗАГРУЗКА В БД
# ============================================================================

def load_requirements(conn, records, source='1C_import'):
    """Загрузка потребностей в БД"""
    cursor = conn.cursor()
    
    print(f"\n=== Загрузка detail_requirements ({len(records)} записей) ===")
    
    # Получаем маппинг деталей по коду
    cursor.execute("SELECT id, code FROM details")
    detail_map = {code: detail_id for detail_id, code in cursor.fetchall()}
    
    # Подготавливаем записи для вставки
    inserts = []
    skipped = 0
    
    for rec in records:
        # Ищем деталь по коду
        detail_id = detail_map.get(rec['detail_code'])
        
        if not detail_id:
            print(f"⚠️  Деталь не найдена: {rec['detail_code']}")
            skipped += 1
            continue
        
        inserts.append((
            detail_id,
            rec['phase'],
            rec['requirement_month'],
            rec['required_quantity'],
            source
        ))
    
    if inserts:
        # Используем UPSERT для обновления существующих записей
        execute_batch(cursor, """
            INSERT INTO detail_requirements (
                detail_id,
                phase,
                requirement_month,
                required_quantity,
                source
            ) VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (detail_id, phase, requirement_month)
            DO UPDATE SET
                required_quantity = EXCLUDED.required_quantity,
                source = EXCLUDED.source,
                updated_at = CURRENT_TIMESTAMP
        """, inserts)
        
        conn.commit()
        print(f"✅ Загружено: {len(inserts)}, Пропущено: {skipped}")
    else:
        print(f"⚠️  Нет записей для загрузки (пропущено: {skipped})")
    
    cursor.close()
                detail_id, phase, requirement_month, required_quantity, source
            )
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (detail_id, phase, requirement_month) 
            DO UPDATE SET 
                required_quantity = EXCLUDED.required_quantity,
                source = EXCLUDED.source,
                updated_at = CURRENT_TIMESTAMP
        """, inserts)
        
        conn.commit()
    
    print(f"✅ Загружено: {len(inserts)}")
    print(f"⚠️  Пропущено: {skipped}")

def load_inventory(conn, records, snapshot_date=None):
    """Загрузка остатков склада в БД"""
    cursor = conn.cursor()
    
    if snapshot_date is None:
        snapshot_date = date.today()
    
    print(f"\n=== Загрузка inventory_snapshots ({len(records)} записей) ===")
    print(f"Дата снапшота: {snapshot_date}")
    
    # Получаем маппинги по кодам
    cursor.execute("SELECT id, code FROM details")
    detail_map = {code: detail_id for detail_id, code in cursor.fetchall()}
    
    cursor.execute("SELECT id, warehouse_name FROM warehouses")
    warehouse_map = {name: wh_id for wh_id, name in cursor.fetchall()}
    
    # Удаляем старые данные за эту дату
    cursor.execute("DELETE FROM inventory_snapshots WHERE snapshot_date = %s", 
                   (snapshot_date,))
    
    inserts = []
    skipped = 0
    
    for rec in records:
        # Находим деталь по коду
        detail_id = detail_map.get(rec['detail_code'])
        
        if not detail_id:
            print(f"⚠️  Деталь не найдена: {rec['detail_code']}")
            skipped += 1
            continue
        
        # Находим склад (используем частичное совпадение или дефолт)
        warehouse_id = None
        for wh_name, wh_id in warehouse_map.items():
            if wh_name in rec['warehouse'] or rec['warehouse'] in wh_name:
                warehouse_id = wh_id
                break
        
        if not warehouse_id:
            warehouse_id = warehouse_map.get('Склад отливок')
        
        inserts.append((
            snapshot_date,
            detail_id,
            rec['characteristic'],  # Фаза обработки
            warehouse_id,
            rec['quantity']
        ))
    
    if inserts:
        execute_batch(cursor, """
            INSERT INTO inventory_snapshots (
                snapshot_date, detail_id, phase, warehouse_id, quantity
            )
            VALUES (%s, %s, %s, %s, %s)
        """, inserts)
        
        conn.commit()
        print(f"✅ Загружено: {len(inserts)}, Пропущено: {skipped}")
    else:
        print(f"⚠️  Нет записей для загрузки (пропущено: {skipped})")
    
    cursor.close()

def load_materials(conn, records, snapshot_date=None):
    """Загрузка остатков металла в БД"""
    cursor = conn.cursor()
    
    if snapshot_date is None:
        snapshot_date = date.today()
    
    print(f"\n=== Загрузка material_inventory_snapshots ({len(records)} записей) ===")
    print(f"Дата снапшота: {snapshot_date}")
    
    # Удаляем старые данные за эту дату
    cursor.execute("DELETE FROM material_inventory_snapshots WHERE snapshot_date = %s", 
                   (snapshot_date,))
    
    inserts = [(snapshot_date, rec['material_type'], rec['quantity_kg']) 
               for rec in records]
    
    if inserts:
        execute_batch(cursor, """
            INSERT INTO material_inventory_snapshots (
                snapshot_date, material_type, quantity_kg
            )
            VALUES (%s, %s, %s)
        """, inserts)
        
        conn.commit()
    
    print(f"✅ Загружено: {len(inserts)}")

# ============================================================================
# CLI
# ============================================================================

def connect_db(connection_string):
    """Подключение к БД"""
    try:
        conn = psycopg2.connect(connection_string)
        conn.autocommit = False
        print(f"✅ Подключено к БД")
        return conn
    except Exception as e:
        print(f"❌ Ошибка подключения: {e}")
        sys.exit(1)

def main():
    parser = argparse.ArgumentParser(
        description='ETL скрипт для импорта данных из 1С в БД',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Примеры использования:

  # Импорт потребностей
  python etl_1c.py -c "postgresql://..." --requirements отливка.xlsx
  
  # Импорт остатков склада
  python etl_1c.py -c "postgresql://..." --inventory остатки.xlsx --date 2025-11-15
  
  # Импорт остатков металла
  python etl_1c.py -c "postgresql://..." --materials металл.xlsx
        """
    )
    
    parser.add_argument('--connection', '-c',
                       help='Connection string (или DATABASE_URL)')
    parser.add_argument('--requirements', '-r',
                       help='Файл с потребностями (Отливка.xlsx)')
    parser.add_argument('--phase', '-p',
                       choices=['ot', 'za', 'dr', 'fr', 'ma', 'all'],
                       help='Фильтр по фазе: ot=отливка, za=зачистка, dr=дробеструй, fr=фрезер, ma=материал, all=все')
    parser.add_argument('--inventory', '-i',
                       help='Файл с остатками склада')
    parser.add_argument('--materials', '-m',
                       help='Файл с остатками металла')
    parser.add_argument('--date', '-d',
                       help='Дата снапшота (YYYY-MM-DD), по умолчанию - сегодня')
    parser.add_argument('--dry-run', action='store_true',
                       help='Парсинг без загрузки в БД')
    
    args = parser.parse_args()
    
    # Проверка параметров
    if not any([args.requirements, args.inventory, args.materials]):
        parser.error("Укажи хотя бы один файл для импорта")
    
    # Connection string
    conn_string = args.connection or os.getenv('DATABASE_URL')
    if not conn_string and not args.dry_run:
        parser.error("Не указан connection string. Используй --connection или DATABASE_URL")
    
    # Дата снапшота
    snapshot_date = None
    if args.date:
        try:
            snapshot_date = datetime.strptime(args.date, '%Y-%m-%d').date()
        except ValueError:
            parser.error("Неверный формат даты. Используй YYYY-MM-DD")
    
    print("=" * 70)
    print("ETL: ИМПОРТ ДАННЫХ ИЗ 1С")
    print("=" * 70)
    
    # Подключение к БД
    conn = None
    if not args.dry_run:
        conn = connect_db(conn_string)
    
    try:
        # Импорт остатков на складах
        if args.inventory:
            filepath = Path(args.inventory)
            if not filepath.exists():
                print(f"❌ Файл не найден: {filepath}")
                sys.exit(1)
            
            print(f"\n📄 Парсинг файла остатков: {filepath}")
            records = parse_inventory_file(filepath)
            print(f"\n✅ Распознано записей: {len(records)}")
            
            if records and not args.dry_run:
                load_inventory(conn, records)
        
        # Импорт потребностей
        if args.requirements:
            filepath = Path(args.requirements)
            if not filepath.exists():
                print(f"❌ Файл не найден: {filepath}")
                sys.exit(1)
            
            phase_filter = args.phase if hasattr(args, 'phase') else None
            print(f"\n📄 Парсинг файла потребностей: {filepath}")
            if phase_filter:
                print(f"   Фильтр по фазе: {phase_filter}")
            records = parse_requirements_file(filepath, phase_filter)
            print(f"\n✅ Распознано записей: {len(records)}")
            
            if records and not args.dry_run:
                load_requirements(conn, records)
        
        # Импорт остатков металла
        if args.materials:
            filepath = Path(args.materials)
            if not filepath.exists():
                print(f"❌ Файл не найден: {filepath}")
                sys.exit(1)
            
            print(f"\n📄 Парсинг файла металла: {filepath}")
            records = parse_materials_file(filepath)
            print(f"  Распознано записей: {len(records)}")
            
            if records and not args.dry_run:
                load_materials(conn, records, snapshot_date)
        
        print("\n" + "=" * 70)
        if args.dry_run:
            print("🔍 DRY RUN - данные НЕ загружены в БД")
        else:
            print("✅ ИМПОРТ ЗАВЕРШЕН")
        print("=" * 70)
        
    except Exception as e:
        if conn:
            conn.rollback()
        print(f"\n❌ ОШИБКА: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        if conn:
            conn.close()

if __name__ == '__main__':
    main()
