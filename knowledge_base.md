# База знаний: Планировщик производства литейного цеха

## 1. Архитектура данных

### Производственный поток
```
Сырьё (металл) 
  ↓
Отливка (на пресс-формах) 
  ↓
Зачистка → Дробеструй → Фрезеровка → Покраска
  ↓
Сборка (комплектация готовых изделий)
```

**Брак**: отдельная фаза, учитывается на складе

### Основные сущности

**Деталь на складе** = (detail_id, phase)
- 21 деталь
- Каждая отливается на 1 форме (6 покупных без формы)
- Каждая входит в 1 сборку

**Оборудование**:
- 15 пресс-форм
- 10 машин (2 литейных, 4 зачистки, 2 дробеструя, 2 фрезера)

**Сборки**: 7 типов готовых изделий

## 2. Поток планирования

```
┌─────────────────────────────────────────────────────────────┐
│ 1. ЗАКАЗЫ                                                   │
│    orders: заказы на сборки (assembly_id, due_date, qty)    │
└─────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────┐
│ 2. ПОТРЕБНОСТИ                                              │
│    detail_requirements: детали по фазам на 9 мес            │
│    Источники: from_orders, 1C_import, manual                │
└─────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────┐
│ 3. ПРЕДВАРИТЕЛЬНЫЙ ПЛАН                                     │
│    tentative_production_plan: неделя/месяц/квартал/9мес     │
│    (start_date, end_date, machine_id, status)               │
│    Status: heuristic/cached/emergency/obsolete              │
│    Более краткосрочный вычисляется из долгосрочного         │
└─────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────┐
│ 4. ТВЁРДЫЙ ПЛАН НА ДЕНЬ                                     │
│    daily_production_plan: план на завтра                    │
│    UNIQUE(plan_date, detail_id, operation, machine_id)      │
│    Перезаписывается через DELETE + INSERT                   │
└─────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────┐
│ 5. ФАКТ                                                     │
│    production_transactions: что сделали                     │
│    (from_phase → to_phase, operator_name)                   │
└─────────────────────────────────────────────────────────────┘
```

## 3. Структура БД (14 таблиц)

### Справочники (5)
```sql
warehouses         -- склады
molds              -- пресс-формы (max_hits)
assemblies         -- сборки (готовые изделия)
details            -- детали (денормализовано: mold_id, qty_per_hit, assembly_id, qty_in_assembly)
machines           -- станки (output_phase)
```

### Параметры производительности (2)
```sql
machine_mold_params    -- для отливки (cycle_duration, loading_duration)
machine_detail_params  -- для остальных фаз (quantity_per_cycle, cycle_duration, loading_duration)
```

### Снапшоты состояния - ежедневно из 1С (3)
```sql
machine_state                   -- состояние машин (config_params JSONB: {"mold_id": 5})
inventory_snapshots             -- остатки (detail_id, phase, warehouse_id, quantity)
material_inventory_snapshots    -- металл (material_type, quantity_kg)
```

### Заказы и планирование (4)
```sql
orders                        -- внешние заказы на сборки
detail_requirements           -- потребности в деталях по фазам
tentative_production_plan     -- предварительный план с периодом
daily_production_plan         -- твёрдый план на день (с UNIQUE constraint)
production_transactions       -- факт выполнения
```

## 4. Ключевые решения

### Денормализация
**Причина**: отношения 1:1
- `details.mold_id` + `qty_per_hit` вместо `mold_details`
- `details.assembly_id` + `qty_in_assembly` вместо `assembly_composition`

### Разделение планов
**Причина**: разная природа данных
- `tentative`: период (start/end), статус, не окончательный
- `daily`: конкретная дата, UNIQUE constraint, твёрдый

### Брак как фаза
**Причина**: упрощение
- Вместо отдельной таблицы `defects`
- `production_transactions` с `to_phase='брак'`
- `inventory_snapshots.phase` включает 'брак'

### Constraints в коде
**Причина**: гибкость
- Safety stock правила → Python переменные
- Физические ограничения → Python переменные
- НЕ хранятся в БД

## 5. Вычисляемые значения

**НЕ хранить в БД** - считать в Python:

```python
# Износ формы
def get_mold_total_hits(mold_id):
    return db.query("""
        SELECT SUM(pt.quantity / d.qty_per_hit)
        FROM production_transactions pt
        JOIN details d ON pt.detail_id = d.id
        WHERE d.mold_id = ? AND pt.operation_type = 'отливка'
    """, mold_id).scalar()

# Расход металла
def get_material_consumption(start_date, end_date):
    return db.query("""
        SELECT SUM(d.weight_kg * pt.quantity)
        FROM production_transactions pt
        JOIN details d ON pt.detail_id = d.id
        WHERE pt.operation_type = 'отливка'
        AND pt.transaction_date BETWEEN ? AND ?
    """, start_date, end_date).scalar()

# Текущий инвентарь
def get_current_inventory(detail_id, phase):
    latest_date = db.query("SELECT MAX(snapshot_date) FROM inventory_snapshots").scalar()
    return db.query("""
        SELECT quantity FROM inventory_snapshots
        WHERE snapshot_date = ? AND detail_id = ? AND phase = ?
    """, latest_date, detail_id, phase).scalar()
```

## 6. Операции планировщика

### Перезапись плана на день
```python
# Защита от дублей через DELETE + INSERT
with db.transaction():
    db.execute("DELETE FROM daily_production_plan WHERE plan_date = ?", tomorrow)
    db.execute("INSERT INTO daily_production_plan ...", records)

# Или через UPSERT
db.execute("""
    INSERT INTO daily_production_plan (plan_date, detail_id, operation, machine_id, quantity_planned)
    VALUES (?, ?, ?, ?, ?)
    ON CONFLICT (plan_date, detail_id, operation, machine_id) 
    DO UPDATE SET quantity_planned = EXCLUDED.quantity_planned
""")
```

### Генерация requirements из orders
```python
def generate_requirements_from_orders():
    for order in db.query("SELECT * FROM orders WHERE due_date >= ?", today):
        assembly = db.query("SELECT * FROM assemblies WHERE id = ?", order.assembly_id).one()
        details = db.query("""
            SELECT * FROM details WHERE assembly_id = ?
        """, assembly.id).all()
        
        for detail in details:
            required_qty = order.quantity * detail.qty_in_assembly
            db.insert("detail_requirements", {
                'detail_id': detail.id,
                'phase': 'покраска',  # готовые детали
                'requirement_month': order.due_date.replace(day=1),
                'required_quantity': required_qty,
                'source': 'from_orders'
            })
```

### Импорт из 1С
```python
def import_from_1c_excel(filepath):
    df = pd.read_excel(filepath)
    for _, row in df.iterrows():
        detail = find_detail_by_nomenclature(row['Номенклатура'])
        db.insert("detail_requirements", {
            'detail_id': detail.id,
            'phase': row['Фаза обработки'],
            'requirement_month': row['Дата запуска'],
            'required_quantity': row['Размещено в заказах'],
            'source': '1C_import'
        })
```

## 7. MVP ограничения

- **Смены**: всегда 1 (рабочий день, 8 часов)
- **Источник данных**: Excel экспорт из 1С (API в будущем)
- **Горизонт**: 9 месяцев вперёд
- **Запас**: 6 дней в месяц на внештатные ситуации
- **Статусы плана**: только базовые (без cancelled/delayed)

## 8. Источники данных

### Из 1С УПП (Excel экспорт):
1. **Остатки склада** → `inventory_snapshots`
2. **Остатки металла** → `material_inventory_snapshots`
3. **Заказы ("Отливка.xls")** → `detail_requirements`

### Ручной ввод:
- Справочники (детали, формы, машины, сборки)
- Параметры производительности
- Состояние машин

### Генерируется планировщиком:
- `detail_requirements` (из orders)
- `tentative_production_plan`
- `daily_production_plan`

## 9. Типичные запросы

```sql
-- План на завтра по машинам
SELECT m.name, d.nomenclature_code, p.operation, p.quantity_planned
FROM daily_production_plan p
JOIN machines m ON p.machine_id = m.id
JOIN details d ON p.detail_id = d.id
WHERE p.plan_date = '2025-11-11'
ORDER BY m.machine_number;

-- Остатки по детали во всех фазах
SELECT d.nomenclature_code, i.phase, SUM(i.quantity) as total
FROM inventory_snapshots i
JOIN details d ON i.detail_id = d.id
WHERE i.snapshot_date = (SELECT MAX(snapshot_date) FROM inventory_snapshots)
GROUP BY d.nomenclature_code, i.phase;

-- Потребности на месяц
SELECT d.nomenclature_code, r.phase, SUM(r.required_quantity) as needed
FROM detail_requirements r
JOIN details d ON r.detail_id = d.id
WHERE r.requirement_month = '2025-12-01'
GROUP BY d.nomenclature_code, r.phase;

-- Факт производства за неделю
SELECT d.nomenclature_code, pt.operation_type, SUM(pt.quantity) as produced
FROM production_transactions pt
JOIN details d ON pt.detail_id = d.id
WHERE pt.transaction_date BETWEEN '2025-11-04' AND '2025-11-10'
GROUP BY d.nomenclature_code, pt.operation_type;
```

## 10. Будущие улучшения (после MVP)

- API интеграция с 1С вместо Excel
- Множественные смены (2-3 смены в день)
- Расширенные статусы планов
- История изменений планов (audit log)
- Оптимизация расписания станков
- Предиктивный анализ поломок
- Real-time мониторинг
