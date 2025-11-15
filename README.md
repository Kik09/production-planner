# PROMPT: Создание системы планирования производства литейного цеха

## Контекст

Разработай схему БД (PostgreSQL) для MVP планировщика производства литейного цеха.

### Производственный процесс
- **Фазы обработки**: отливка → зачистка → дробеструй → фрезеровка → покраска
- **Брак**: отдельная фаза (учитывается на складе)
- **Деталь на складе** = (detail_id + phase)

### Оборудование
- 15 пресс-форм (отливка)
- 10 машин: 2 литейных, 4 зачистки, 2 дробеструя, 2 фрезера
- Каждая деталь отливается на **одной форме** (1:1)
- Каждая деталь входит в **одну сборку** (1:1)

### Логика планирования

```
1. Внешние заказы на сборки → orders (assembly_id, due_date, quantity)
2. Анализ → detail_requirements (детали по фазам на 9 мес, источники: orders/1С/manual)
3. Предварительный план → tentative_production_plan (неделя/месяц/9мес, start_date, end_date, status: heuristic/cached/emergency/obsolete)
4. Твёрдый план → daily_production_plan (на завтра, UNIQUE constraint на plan_date+detail_id+operation+machine_id)
5. Факт → production_transactions (from_phase, to_phase)
```

### Ключевые принципы

**Денормализация:**
- `details` содержит: mold_id, qty_per_hit, assembly_id, qty_in_assembly
- Нет отдельных таблиц mold_details, assembly_composition

**Снапшоты (ежедневно из 1С):**
- `machine_state` (config_params JSONB: {"mold_id": 5} для литейных)
- `inventory_snapshots` (snapshot_date, detail_id, phase, warehouse_id, quantity)
- `material_inventory_snapshots` (snapshot_date, material_type, quantity_kg)

**Constraints и правила:**
- Хранятся в коде Python, НЕ в БД
- Safety stock, физические ограничения → переменные в планировщике

**Параметры производительности:**
- `machine_mold_params` (для отливки): cycle_duration_minutes, loading_duration_minutes
- `machine_detail_params` (для остальных): quantity_per_cycle, cycle_duration_minutes, loading_duration_minutes

**Смены:**
- shift_number DEFAULT 1 (для MVP - рабочий день)

### Вычисляемые значения (НЕ хранить в БД)

```sql
-- Износ формы
SELECT SUM(pt.quantity / d.qty_per_hit) as total_hits
FROM production_transactions pt
JOIN details d ON pt.detail_id = d.id
WHERE d.mold_id = X AND pt.operation_type = 'отливка'

-- Расход металла
SELECT SUM(d.weight_kg * pt.quantity)
FROM production_transactions pt
JOIN details d ON pt.detail_id = d.id
WHERE pt.operation_type = 'отливка'
AND pt.transaction_date BETWEEN start_date AND end_date
```

### Операции планировщика

```python
# Перезапись плана на день (защита от дублей)
DELETE FROM daily_production_plan WHERE plan_date = ?
INSERT INTO daily_production_plan ...

# Или UPSERT
INSERT ... ON CONFLICT (plan_date, detail_id, operation, machine_id) 
DO UPDATE SET quantity_planned = EXCLUDED.quantity_planned
```

## Требования к схеме

1. **14 таблиц**: справочники (5), параметры (2), снапшоты (3), заказы+планы (4)
2. Комментарии в SQL должны быть написаны **человеком**, не AI-стилем
3. Все CHECK constraints с явными значениями
4. Индексы на FK и часто используемые поля
5. ON DELETE CASCADE/SET NULL по логике
6. UNIQUE constraints где нужно

## Deliverables

1. `schema_final.sql` - полная SQL схема с комментариями
2. `schema_diagram.mermaid` - ER диаграмма
3. Краткое описание структуры (Markdown)

## Особенности реализации

- PostgreSQL (используй SERIAL, JSONB)
- Даты: DATE (не TIMESTAMP для бизнес-дат)
- Decimal для весов/количеств с дробями
- VARCHAR с разумными лимитами
- TEXT для notes/description
- created_at/updated_at где логично

## Пример структуры таблиц

```sql
-- Справочники: warehouses, molds, assemblies, details, machines
-- Параметры: machine_mold_params, machine_detail_params
-- Снапшоты: machine_state, inventory_snapshots, material_inventory_snapshots
-- Заказы+планы: orders, detail_requirements, tentative_production_plan, daily_production_plan
-- Факт: production_transactions
```

Создай чистую, продуманную схему без избыточности. Логика в коде, данные в БД.
