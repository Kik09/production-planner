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

def is_empty_row(row):
    """Проверка что строка пустая"""
    return row.isna().all() or (row.astype(str).str.strip() == '').all()


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
    
    # 1. Пропускаем служебные строки (описание отчёта)
    current_row = 0
    service_patterns = [r'Группировки строк', r'Отбор', r'Упорядочивание', 
                       r'Оформление', r'Настройки']
    
    while current_row < min(15, nrows):
        row = df.iloc[current_row]
        if is_empty_row(row):
            current_row += 1
            continue
        
        # Проверяем первую непустую ячейку
        first_cell = None
        for col in range(ncols):
            val = str(row[col]) if pd.notna(row[col]) else ''
            if val.strip():
                first_cell = val
                break
        
        # Служебная строка?
        if first_cell and any(re.search(pattern, first_cell) for pattern in service_patterns):
            print(f"⏭️  Пропуск служебной строки {current_row}: {first_cell[:50]}...")
            current_row += 1
            continue
        
        # Заголовки найдены?
        if first_cell and re.search(r'Характеристика|Номенклатура|Заказ', first_cell):
            break
        
        current_row += 1
    
    # 2. Парсим заголовки - это иерархия (может быть несколько строк!)
    hierarchy_levels = []
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
            
            # Ищем первую непустую ячейку в строке
            for col in range(ncols):
                val = str(row[col]) if pd.notna(row[col]) else ''
                val = val.strip()
                if val and val != '-':
                    hierarchy_levels.append({
                        'col': col,
                        'name': val
                    })
                    print(f"   Уровень {level_idx}: колонка {col} - '{val}'")
                    level_idx += 1
                    break  # Только первая непустая ячейка
            
            header_row += 1
    
    if not hierarchy_levels:
        print("❌ Не найдены заголовки иерархии")
        return []
    
    # 3. Начало данных - после заголовков (header_row уже указывает на пустую строку или первую строку данных)
    start_row = header_row
    while start_row < nrows and is_empty_row(df.iloc[start_row]):
        start_row += 1
    
    print(f"\n📊 Начало данных: строка {start_row}\n")
    
    # 4. Парсим данные: уровень по колонке, тип по паттерну
    records = []
    state = {'phase': None, 'assembly': None, 'detail_code': None}
    
    # Паттерны для типов данных
    phase_pat = re.compile(r'^(Отливка|Зачистка|Дробеструй|Токарка|Фрезеровка|Слесарка|Алюминий)')
    detail_pat = re.compile(r'К\d+\.\d+\.\d+')
    date_pat = re.compile(r'\d{2}\.\d{2}\.\d{4}')
    
    for i in range(start_row, nrows):
        row = df.iloc[i]
        if is_empty_row(row):
            continue
        
        # Находим первую непустую ячейку и её колонку
        cell_value = None
        cell_col = None
        for col in range(ncols):
            val = row[col]
            if pd.notna(val) and str(val).strip() and str(val).strip() != '-':
                cell_value = str(val).strip()
                cell_col = col
                break
        
        if not cell_value:
            continue
        
        # Определяем уровень по колонке
        current_level = None
        for level_idx, level in enumerate(hierarchy_levels):
            if level['col'] == cell_col:
                current_level = level_idx
                break
        
        if current_level is None:
            continue
        
        print(f"Строка {i:3d} | Уровень {current_level} (col {cell_col}): {cell_value[:50]}")
        
        # Обработка по уровню + паттерну
        if current_level == 0:  # Фаза
            if phase_pat.match(cell_value):
                phase = cell_value.split()[0].lower()
                if phase == 'алюминий': phase = 'материал'
                elif phase == 'токарка': phase = 'фрезеровка'
                state['phase'] = phase
                state['assembly'] = None
                state['detail_code'] = None
        
        elif current_level == 1:  # Сборка/Артикул
            state['assembly'] = cell_value
            state['detail_code'] = None
        
        elif current_level == 3:  # Деталь
            match = re.search(r'\((К\d+\.\d+\.\d+[^\)]*)\)', cell_value)
            if match:
                state['detail_code'] = match.group(1)
            else:
                match = detail_pat.search(cell_value)
                if match:
                    state['detail_code'] = match.group(0)
        
        elif current_level == 4:  # Дата
            if date_pat.search(cell_value) and state['detail_code'] and state['phase']:
                try:
                    req_date = datetime.strptime(cell_value.split()[0], '%d.%m.%Y').date()
                    req_month = req_date.replace(day=1)
                    
                    # Количество в следующих колонках
                    quantity = 0
                    for col in range(cell_col + 1, ncols):
                        val = row[col]
                        if pd.notna(val) and val != '-':
                            try:
                                quantity = int(float(str(val).replace(',', '.')))
                                break
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

def parse_inventory_file(filepath):
    """
    Парсинг файла остатков склада
    
    Ожидаемая структура:
    - Колонки: Номенклатура | Фаза | Склад | Количество
    
    Возвращает: список dict с полями:
        - detail_name: название детали
        - phase: фаза обработки
        - warehouse_name: название склада
        - quantity: количество
    """
    # Пытаемся найти заголовки
    df = pd.read_excel(filepath, sheet_name=0, header=None)
    
    # Ищем строку с заголовками (содержит "Номенклатура")
    header_row = None
    for i in range(min(20, len(df))):
        row_str = ' '.join([str(x) for x in df.iloc[i].tolist() if pd.notna(x)])
        if 'Номенклатура' in row_str or 'номенклатура' in row_str.lower():
            header_row = i
            break
    
    if header_row is None:
        raise ValueError("Не найдена строка с заголовками (должна содержать 'Номенклатура')")
    
    # Читаем с найденными заголовками
    df = pd.read_excel(filepath, sheet_name=0, header=header_row)
    
    records = []
    for _, row in df.iterrows():
        # Пропускаем пустые строки
        if pd.isna(row.get('Номенклатура')):
            continue
        
        detail_name = str(row.get('Номенклатура', '')).strip()
        phase = str(row.get('Фаза', 'отливка')).strip().lower()
        warehouse = str(row.get('Склад', 'Склад отливок')).strip()
        quantity = row.get('Количество', 0)
        
        if detail_name and quantity > 0:
            records.append({
                'detail_name': detail_name,
                'phase': phase,
                'warehouse_name': warehouse,
                'quantity': int(quantity)
            })
    
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
    
    # Получаем маппинг деталей
    cursor.execute("SELECT id, name FROM details")
    detail_map = {name: detail_id for detail_id, name in cursor.fetchall()}
    
    # Подготавливаем записи для вставки
    inserts = []
    skipped = 0
    
    for rec in records:
        # Ищем деталь по имени (может содержать доп. текст)
        detail_id = None
        for db_name, db_id in detail_map.items():
            if db_name in rec['detail_name'] or rec['detail_name'] in db_name:
                detail_id = db_id
                break
        
        if not detail_id:
            print(f"⚠️  Деталь не найдена: {rec['detail_name']}")
            skipped += 1
            continue
        
        # Округляем дату до первого числа месяца
        req_month = rec['requirement_date'].replace(day=1)
        
        inserts.append((
            detail_id,
            rec['phase'],
            req_month,
            rec['required_quantity'],
            source
        ))
    
    if inserts:
        # Используем UPSERT для обновления существующих записей
        execute_batch(cursor, """
            INSERT INTO detail_requirements (
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
    
    # Получаем маппинги
    cursor.execute("SELECT id, name FROM details")
    detail_map = {name: detail_id for detail_id, name in cursor.fetchall()}
    
    cursor.execute("SELECT id, warehouse_name FROM warehouses")
    warehouse_map = {name: wh_id for wh_id, name in cursor.fetchall()}
    
    # Удаляем старые данные за эту дату
    cursor.execute("DELETE FROM inventory_snapshots WHERE snapshot_date = %s", 
                   (snapshot_date,))
    
    inserts = []
    skipped = 0
    
    for rec in records:
        # Находим деталь
        detail_id = None
        for db_name, db_id in detail_map.items():
            if db_name in rec['detail_name'] or rec['detail_name'] in db_name:
                detail_id = db_id
                break
        
        if not detail_id:
            print(f"⚠️  Деталь не найдена: {rec['detail_name']}")
            skipped += 1
            continue
        
        # Находим склад (или используем дефолтный)
        warehouse_id = warehouse_map.get(rec['warehouse_name'], 
                                         warehouse_map.get('Склад отливок'))
        
        inserts.append((
            snapshot_date,
            detail_id,
            rec['phase'],
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
    
    print(f"✅ Загружено: {len(inserts)}")
    print(f"⚠️  Пропущено: {skipped}")

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
        
        # Импорт остатков склада
        if args.inventory:
            filepath = Path(args.inventory)
            if not filepath.exists():
                print(f"❌ Файл не найден: {filepath}")
                sys.exit(1)
            
            print(f"\n📄 Парсинг файла остатков: {filepath}")
            records = parse_inventory_file(filepath)
            print(f"  Распознано записей: {len(records)}")
            
            if records and not args.dry_run:
                load_inventory(conn, records, snapshot_date)
        
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
