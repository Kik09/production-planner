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

def parse_requirements_file(filepath):
    """
    Парсинг файла "Анализ обеспеченности заказов" (Отливка.xlsx)
    
    Структура файла:
    - Строка 7-9: заголовки
    - Далее: иерархия (фаза → сборка → деталь → даты)
    
    Возвращает: список dict с полями:
        - detail_name: название детали
        - phase: фаза обработки  
        - requirement_date: дата потребности
        - required_quantity: количество
        - nzp: НЗП (незавершенное производство)
        - reserved: зарезервировано на складе
        - ordered: размещено в заказах
    """
    df = pd.read_excel(filepath, sheet_name=0, header=None)
    
    records = []
    current_phase = None
    current_assembly = None
    current_detail = None
    
    # Начинаем со строки 12 (после заголовков)
    for i in range(12, len(df)):
        row = df.iloc[i]
        
        # Первая колонка всегда NaN, данные во второй
        name = row[1]
        
        if pd.isna(name):
            continue
            
        name = str(name).strip()
        
        # Пропускаем пустые строки
        if not name or name == '-':
            continue
        
        # Определяем тип строки
        
        # 1. Фаза обработки (Дробеструй, Зачистка и т.д.)
        if name in ['Дробеструй', 'Зачистка', 'Отливка', 'Фрезеровка', 'Токарка', 
                    'Покраска', 'Слесарка', 'Алюминий 4 и 5 месяцев',
                    'Алюминий и сплавы алюминиевые']:
            current_phase = name if name not in ['Алюминий 4 и 5 месяцев', 
                                                   'Алюминий и сплавы алюминиевые'] else None
            current_assembly = None
            current_detail = None
            continue
        
        # 2. Сборка (число или название типа "4523", "Иволга кресло")
        if (isinstance(name, (int, float)) or 
            any(x in name for x in ['кресло', 'Лестница', 'Комплект', 'Опора', 'Привод'])):
            current_assembly = name
            current_detail = None
            continue
        
        # 3. Деталь (содержит код типа К03.02.004)
        if re.search(r'К\d+\.\d+\.\d+', name):
            current_detail = name
            continue
        
        # 4. Дата (формат 01.02.2026 0:00:00 или дата)
        if current_detail and current_phase:
            # Проверяем, это дата?
            try:
                # Пытаемся распарсить как дату
                if isinstance(name, datetime):
                    req_date = name.date()
                else:
                    # Формат "01.02.2026 0:00:00"
                    req_date = datetime.strptime(name.split()[0], '%d.%m.%Y').date()
                
                # Извлекаем значения
                potreb = row[2]  # Потребность
                nzp = row[3]     # НЗП
                reserved = row[4]  # Зарезервировано
                ordered = row[5]   # Размещено в заказах
                
                # Конвертируем '-' в 0
                def to_num(val):
                    if pd.isna(val) or val == '-':
                        return 0
                    return int(val)
                
                records.append({
                    'detail_name': current_detail,
                    'phase': current_phase.lower(),
                    'requirement_date': req_date,
                    'required_quantity': to_num(potreb),
                    'nzp': to_num(nzp),
                    'reserved': to_num(reserved),
                    'ordered': to_num(ordered)
                })
                
            except (ValueError, AttributeError):
                # Не дата - пропускаем
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
            
            print(f"\n📄 Парсинг файла потребностей: {filepath}")
            records = parse_requirements_file(filepath)
            print(f"  Распознано записей: {len(records)}")
            
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
