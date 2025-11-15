#!/usr/bin/env python3
"""
Скрипт инициализации БД литейного цеха из мастер-файла Excel

Использование:
    python init_db.py --connection "postgresql://user:pass@host:port/dbname" --data master_data.xlsx
    
Или с переменными окружения:
    export DATABASE_URL="postgresql://user:pass@host:port/dbname"
    python init_db.py --data master_data.xlsx
"""

import argparse
import sys
import os
from pathlib import Path
import pandas as pd
import psycopg2
from psycopg2.extras import execute_batch
from datetime import datetime

def connect_db(connection_string):
    """Подключение к БД"""
    try:
        conn = psycopg2.connect(connection_string)
        conn.autocommit = False
        print(f"✅ Подключено к БД")
        return conn
    except Exception as e:
        print(f"❌ Ошибка подключения к БД: {e}")
        sys.exit(1)

def load_excel(filepath):
    """Загрузка данных из Excel"""
    try:
        data = {}
        xl = pd.ExcelFile(filepath)
        for sheet_name in xl.sheet_names:
            data[sheet_name] = pd.read_excel(filepath, sheet_name=sheet_name)
            print(f"  📄 {sheet_name}: {len(data[sheet_name])} строк")
        print(f"✅ Загружен файл: {filepath}")
        return data
    except Exception as e:
        print(f"❌ Ошибка чтения файла {filepath}: {e}")
        sys.exit(1)

def init_warehouses(conn, df):
    """Инициализация складов"""
    cursor = conn.cursor()
    
    print("\n=== Инициализация warehouses ===")
    
    # Очистка таблицы
    cursor.execute("TRUNCATE TABLE warehouses RESTART IDENTITY CASCADE")
    
    # Вставка данных
    records = []
    for _, row in df.iterrows():
        records.append((
            row['warehouse_name'],
            row['warehouse_type'],
            None  # notes
        ))
    
    execute_batch(cursor, """
        INSERT INTO warehouses (warehouse_name, warehouse_type, notes)
        VALUES (%s, %s, %s)
    """, records)
    
    conn.commit()
    print(f"✅ Добавлено складов: {len(records)}")

def init_molds(conn, df):
    """Инициализация пресс-форм"""
    cursor = conn.cursor()
    
    print("\n=== Инициализация molds ===")
    
    cursor.execute("TRUNCATE TABLE molds RESTART IDENTITY CASCADE")
    
    records = []
    for _, row in df.iterrows():
        records.append((
            int(row['mold_number']),
            row['name'],
            row['install_date'] if pd.notna(row['install_date']) else None,
            int(row['max_hits']),
            row['status']
        ))
    
    execute_batch(cursor, """
        INSERT INTO molds (mold_number, name, install_date, max_hits, status)
        VALUES (%s, %s, %s, %s, %s)
    """, records)
    
    conn.commit()
    print(f"✅ Добавлено форм: {len(records)}")

def init_assemblies(conn, df):
    """Инициализация сборок"""
    cursor = conn.cursor()
    
    print("\n=== Инициализация assemblies ===")
    
    cursor.execute("TRUNCATE TABLE assemblies RESTART IDENTITY CASCADE")
    
    records = []
    for _, row in df.iterrows():
        records.append((row['name'],))
    
    execute_batch(cursor, """
        INSERT INTO assemblies (name)
        VALUES (%s)
    """, records)
    
    conn.commit()
    print(f"✅ Добавлено сборок: {len(records)}")

def init_details(conn, df):
    """Инициализация деталей"""
    cursor = conn.cursor()
    
    print("\n=== Инициализация details ===")
    
    cursor.execute("TRUNCATE TABLE details RESTART IDENTITY CASCADE")
    
    # Получаем ID форм и сборок
    cursor.execute("SELECT id, mold_number FROM molds")
    mold_map = {mold_num: mold_id for mold_id, mold_num in cursor.fetchall()}
    
    cursor.execute("SELECT id, name FROM assemblies")
    assembly_map = {name: asm_id for asm_id, name in cursor.fetchall()}
    
    records = []
    for _, row in df.iterrows():
        # Определяем mold_id
        mold_id = None
        if pd.notna(row['mold_number']):
            mold_num = int(row['mold_number'])
            mold_id = mold_map.get(mold_num)
        
        # Определяем assembly_id
        assembly_id = assembly_map.get(row['assembly_name'])
        
        records.append((
            row['nomenclature_code'],
            row['name'],
            float(row['weight_kg']),
            row['material_type'],
            bool(row['requires_painting']),
            mold_id,
            float(row['qty_per_hit']) if pd.notna(row['qty_per_hit']) else None,
            assembly_id,
            int(row['qty_in_assembly']) if pd.notna(row['qty_in_assembly']) else None
        ))
    
    execute_batch(cursor, """
        INSERT INTO details (
            nomenclature_code, name, weight_kg, material_type, requires_painting,
            mold_id, qty_per_hit, assembly_id, qty_in_assembly
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, records)
    
    conn.commit()
    print(f"✅ Добавлено деталей: {len(records)}")

def init_machines(conn, df):
    """Инициализация машин"""
    cursor = conn.cursor()
    
    print("\n=== Инициализация machines ===")
    
    cursor.execute("TRUNCATE TABLE machines RESTART IDENTITY CASCADE")
    
    records = []
    for _, row in df.iterrows():
        records.append((
            int(row['machine_number']),
            row['name'],
            row['output_phase'],
            row['status']
        ))
    
    execute_batch(cursor, """
        INSERT INTO machines (machine_number, name, output_phase, status)
        VALUES (%s, %s, %s, %s)
    """, records)
    
    conn.commit()
    print(f"✅ Добавлено машин: {len(records)}")

def init_machine_mold_params(conn, df):
    """Инициализация параметров машина-форма"""
    cursor = conn.cursor()
    
    print("\n=== Инициализация machine_mold_params ===")
    
    cursor.execute("TRUNCATE TABLE machine_mold_params RESTART IDENTITY CASCADE")
    
    # Получаем ID машин и форм
    cursor.execute("SELECT id, machine_number FROM machines")
    machine_map = {num: mid for mid, num in cursor.fetchall()}
    
    cursor.execute("SELECT id, mold_number FROM molds")
    mold_map = {num: mid for mid, num in cursor.fetchall()}
    
    records = []
    for _, row in df.iterrows():
        machine_id = machine_map.get(int(row['machine_number']))
        mold_id = mold_map.get(int(row['mold_number']))
        
        if machine_id and mold_id:
            records.append((
                machine_id,
                mold_id,
                int(row['cycle_duration_minutes']),
                int(row['loading_duration_minutes'])
            ))
    
    execute_batch(cursor, """
        INSERT INTO machine_mold_params (
            machine_id, mold_id, cycle_duration_minutes, loading_duration_minutes
        )
        VALUES (%s, %s, %s, %s)
    """, records)
    
    conn.commit()
    print(f"✅ Добавлено параметров машина-форма: {len(records)}")

def init_machine_detail_params(conn, df):
    """Инициализация параметров машина-деталь"""
    cursor = conn.cursor()
    
    print("\n=== Инициализация machine_detail_params ===")
    
    cursor.execute("TRUNCATE TABLE machine_detail_params RESTART IDENTITY CASCADE")
    
    # Получаем ID машин и деталей
    cursor.execute("SELECT id, machine_number FROM machines")
    machine_map = {num: mid for mid, num in cursor.fetchall()}
    
    cursor.execute("SELECT id, nomenclature_code FROM details")
    detail_map = {code: did for did, code in cursor.fetchall()}
    
    records = []
    for _, row in df.iterrows():
        machine_id = machine_map.get(int(row['machine_number']))
        detail_id = detail_map.get(row['nomenclature_code'])
        
        if machine_id and detail_id:
            records.append((
                machine_id,
                detail_id,
                int(row['quantity_per_cycle']),
                int(row['cycle_duration_minutes']),
                int(row['loading_duration_minutes'])
            ))
    
    execute_batch(cursor, """
        INSERT INTO machine_detail_params (
            machine_id, detail_id, quantity_per_cycle, 
            cycle_duration_minutes, loading_duration_minutes
        )
        VALUES (%s, %s, %s, %s, %s)
    """, records)
    
    conn.commit()
    print(f"✅ Добавлено параметров машина-деталь: {len(records)}")

def verify_data(conn):
    """Проверка загруженных данных"""
    cursor = conn.cursor()
    
    print("\n=== Проверка данных ===")
    
    tables = [
        'warehouses', 'molds', 'assemblies', 'details', 'machines',
        'machine_mold_params', 'machine_detail_params'
    ]
    
    for table in tables:
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        count = cursor.fetchone()[0]
        print(f"  {table}: {count} записей")

def main():
    parser = argparse.ArgumentParser(description='Инициализация БД литейного цеха')
    parser.add_argument('--connection', '-c', 
                       help='Connection string для БД (или используй DATABASE_URL)')
    parser.add_argument('--data', '-d', required=True,
                       help='Путь к Excel файлу с данными')
    parser.add_argument('--dry-run', action='store_true',
                       help='Проверка без записи в БД')
    
    args = parser.parse_args()
    
    # Connection string
    conn_string = args.connection or os.getenv('DATABASE_URL')
    if not conn_string:
        print("❌ Не указан connection string. Используй --connection или DATABASE_URL")
        sys.exit(1)
    
    # Проверка файла
    data_file = Path(args.data)
    if not data_file.exists():
        print(f"❌ Файл не найден: {data_file}")
        sys.exit(1)
    
    print("=" * 60)
    print("ИНИЦИАЛИЗАЦИЯ БД ЛИТЕЙНОГО ЦЕХА")
    print("=" * 60)
    
    # Загрузка данных
    print("\n📂 Загрузка данных из Excel...")
    data = load_excel(data_file)
    
    if args.dry_run:
        print("\n🔍 DRY RUN режим - данные НЕ будут записаны в БД")
        return
    
    # Подключение к БД
    print("\n🔌 Подключение к БД...")
    conn = connect_db(conn_string)
    
    try:
        # Инициализация таблиц
        init_warehouses(conn, data['warehouses'])
        init_molds(conn, data['molds'])
        init_assemblies(conn, data['assemblies'])
        init_details(conn, data['details'])
        init_machines(conn, data['machines'])
        init_machine_mold_params(conn, data['machine_mold_params'])
        init_machine_detail_params(conn, data['machine_detail_params'])
        
        # Проверка
        verify_data(conn)
        
        print("\n" + "=" * 60)
        print("✅ ИНИЦИАЛИЗАЦИЯ ЗАВЕРШЕНА УСПЕШНО")
        print("=" * 60)
        
    except Exception as e:
        conn.rollback()
        print(f"\n❌ ОШИБКА: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        conn.close()

if __name__ == '__main__':
    main()
