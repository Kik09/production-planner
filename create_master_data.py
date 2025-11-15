"""
Скрипт для создания мастер-файла инициализации справочников
"""
import pandas as pd
import numpy as np
from datetime import datetime

# ============================================================================
# 1. СКЛАДЫ
# ============================================================================
warehouses = pd.DataFrame([
    {'warehouse_name': 'Литейный цех', 'warehouse_type': 'production'},
    {'warehouse_name': 'Склад отливок', 'warehouse_type': 'storage'},
    {'warehouse_name': 'Склад готовой продукции', 'warehouse_type': 'storage'},
    {'warehouse_name': 'Брак', 'warehouse_type': 'defects'}
])

# ============================================================================
# 2. ПРЕСС-ФОРМЫ
# ============================================================================
molds = pd.DataFrame([
    {'mold_number': i, 'name': f'Форма №{i}', 'max_hits': 100000, 'status': 'active', 
     'install_date': '2023-01-01'}
    for i in range(1, 16)
])

# ============================================================================
# 3. СБОРКИ
# ============================================================================
assemblies = pd.DataFrame([
    {'name': 'Иволга кресло'},
    {'name': '4523'},
    {'name': 'Лестница'},
    {'name': 'Комплект каркаса'},
    {'name': 'Опора дивана'},
    {'name': 'Привод подъемный'},
    {'name': 'Лестница Габарит Т'}
])

# ============================================================================
# 4. ДЕТАЛИ
# ============================================================================
# Читаем из существующего файла
source_file = '/mnt/project/База_данных_Литейный_цех.xlsx'
df_source = pd.read_excel(source_file, sheet_name='Выжимка Данных')

# Создаем номенклатурные коды (извлекаем из имени детали)
def extract_nomenclature(name):
    """Извлекает номенклатурный код типа К12.05.031.2"""
    import re
    match = re.search(r'К\d+\.\d+\.\d+[\.\-]?\d*', str(name))
    return match.group(0) if match else name

details = pd.DataFrame({
    'nomenclature_code': df_source['Наименование детали'].apply(extract_nomenclature),
    'name': df_source['Наименование детали'],
    'weight_kg': df_source['Вес детали фактический, г:'] / 1000,  # г → кг
    'material_type': 'Алюминий',
    'mold_number': df_source['Номер пресс-формы'].replace({np.nan: None}),
    'qty_per_hit': df_source['Кол-во деталей на форме:'],
    'assembly_name': df_source['Наименование готового изделия:'],
    'qty_in_assembly': df_source['Кол-во в готовом изделии:']
})

# Добавляем информацию о покраске из листа "База данных"
df_ops = pd.read_excel(source_file, sheet_name='База данных', header=1)
df_ops = df_ops.iloc[1:]  # Пропускаем строку "Наличие:"
df_ops.columns = ['mold_number', 'detail_name', 'col2', 'weight_g', 'col4', 
                  'assembly_name', 'qty_in_assembly', 'col7', 'col8',
                  'op_otlivka', 'col10', 'op_zachistka', 'col12', 'op_drobestruy', 
                  'col14', 'col15', 'col16', 'op_tokarka', 'op_frezerovka', 
                  'op_slesarka', 'op_pokraska']

# Маппинг покраски по имени детали
paint_map = df_ops.set_index('detail_name')['op_pokraska'].to_dict()
details['requires_painting'] = details['name'].map(paint_map).notna()

print("=== WAREHOUSES ===")
print(warehouses)
print(f"\nTotal: {len(warehouses)}")

print("\n=== MOLDS ===")
print(molds.head())
print(f"\nTotal: {len(molds)}")

print("\n=== ASSEMBLIES ===")
print(assemblies)
print(f"\nTotal: {len(assemblies)}")

print("\n=== DETAILS ===")
print(details.head(10))
print(f"\nTotal: {len(details)}")
print(f"С формами: {details['mold_number'].notna().sum()}")
print(f"Покупные: {details['mold_number'].isna().sum()}")
print(f"Требуют покраски: {details['requires_painting'].sum()}")

# ============================================================================
# 5. МАШИНЫ
# ============================================================================
machines = pd.DataFrame([
    # Литейные машины
    {'machine_number': 1, 'name': 'Литейная машина №1', 'output_phase': 'отливка', 'status': 'active'},
    {'machine_number': 2, 'name': 'Литейная машина №2', 'output_phase': 'отливка', 'status': 'active'},
    
    # Зачистные станки
    {'machine_number': 3, 'name': 'Зачистной станок №1', 'output_phase': 'зачистка', 'status': 'active'},
    {'machine_number': 4, 'name': 'Зачистной станок №2', 'output_phase': 'зачистка', 'status': 'active'},
    {'machine_number': 5, 'name': 'Зачистной станок №3', 'output_phase': 'зачистка', 'status': 'active'},
    {'machine_number': 6, 'name': 'Зачистной станок №4', 'output_phase': 'зачистка', 'status': 'active'},
    
    # Дробеструйные
    {'machine_number': 7, 'name': 'Дробеструй №1', 'output_phase': 'дробеструй', 'status': 'active'},
    {'machine_number': 8, 'name': 'Дробеструй №2', 'output_phase': 'дробеструй', 'status': 'active'},
    
    # Фрезеры
    {'machine_number': 9, 'name': 'Фрезер №1', 'output_phase': 'фрезеровка', 'status': 'active'},
    {'machine_number': 10, 'name': 'Фрезер №2', 'output_phase': 'фрезеровка', 'status': 'active'},
])

print("\n=== MACHINES ===")
print(machines)
print(f"\nTotal: {len(machines)}")

# ============================================================================
# 6. ПАРАМЕТРЫ ПРОИЗВОДИТЕЛЬНОСТИ (примерные для MVP)
# ============================================================================

# Параметры для литейных машин (machine_mold_params)
machine_mold_params_data = []
for machine_id in [1, 2]:  # Литейные машины
    for mold_num in range(1, 16):  # 15 форм
        machine_mold_params_data.append({
            'machine_number': machine_id,
            'mold_number': mold_num,
            'cycle_duration_minutes': 5,  # 5 минут на цикл
            'loading_duration_minutes': 2  # 2 минуты на загрузку
        })

machine_mold_params = pd.DataFrame(machine_mold_params_data)

# Параметры для остальных машин (machine_detail_params)
# Упрощаем: для MVP задаем одинаковые параметры для всех деталей
machine_detail_params_data = []

# Зачистка (машины 3-6)
for machine_id in [3, 4, 5, 6]:
    for _, detail in details.iterrows():
        machine_detail_params_data.append({
            'machine_number': machine_id,
            'nomenclature_code': detail['nomenclature_code'],
            'quantity_per_cycle': 10,  # 10 деталей за цикл
            'cycle_duration_minutes': 15,
            'loading_duration_minutes': 5
        })

# Дробеструй (машины 7-8)
for machine_id in [7, 8]:
    for _, detail in details.iterrows():
        machine_detail_params_data.append({
            'machine_number': machine_id,
            'nomenclature_code': detail['nomenclature_code'],
            'quantity_per_cycle': 20,
            'cycle_duration_minutes': 10,
            'loading_duration_minutes': 3
        })

# Фрезеровка (машины 9-10)
for machine_id in [9, 10]:
    for _, detail in details.iterrows():
        machine_detail_params_data.append({
            'machine_number': machine_id,
            'nomenclature_code': detail['nomenclature_code'],
            'quantity_per_cycle': 5,
            'cycle_duration_minutes': 20,
            'loading_duration_minutes': 5
        })

machine_detail_params = pd.DataFrame(machine_detail_params_data)

print("\n=== MACHINE_MOLD_PARAMS ===")
print(machine_mold_params.head())
print(f"\nTotal: {len(machine_mold_params)}")

print("\n=== MACHINE_DETAIL_PARAMS ===")
print(machine_detail_params.head())
print(f"\nTotal: {len(machine_detail_params)}")

# ============================================================================
# СОХРАНЕНИЕ В EXCEL
# ============================================================================
output_file = '/home/claude/master_data.xlsx'

with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
    warehouses.to_excel(writer, sheet_name='warehouses', index=False)
    molds.to_excel(writer, sheet_name='molds', index=False)
    assemblies.to_excel(writer, sheet_name='assemblies', index=False)
    details.to_excel(writer, sheet_name='details', index=False)
    machines.to_excel(writer, sheet_name='machines', index=False)
    machine_mold_params.to_excel(writer, sheet_name='machine_mold_params', index=False)
    machine_detail_params.to_excel(writer, sheet_name='machine_detail_params', index=False)

print(f"\n✅ Файл создан: {output_file}")
print("\nЛисты:")
print("  - warehouses (4 склада)")
print("  - molds (15 форм)")
print("  - assemblies (7 сборок)")
print(f"  - details ({len(details)} деталей)")
print(f"  - machines ({len(machines)} машин)")
print(f"  - machine_mold_params ({len(machine_mold_params)} записей)")
print(f"  - machine_detail_params ({len(machine_detail_params)} записей)")
