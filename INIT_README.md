# Инициализация БД литейного цеха

## Быстрый старт

### 1. Подготовка данных

Запусти скрипт создания мастер-файла:

```bash
python3 create_master_data.py
```

Результат: `master_data.xlsx` с листами:
- `warehouses` - склады (4 записи)
- `molds` - пресс-формы (15 записей)
- `assemblies` - сборки (7 записей)
- `details` - детали (21 запись)
- `machines` - машины (10 записей)
- `machine_mold_params` - параметры для отливки (30 записей)
- `machine_detail_params` - параметры для остальных операций (168 записей)

### 2. Создание БД

Накати SQL схему:

```bash
psql -U postgres -d foundry_db -f schema_final.sql
```

Или через Supabase dashboard:
- SQL Editor → Открыть `schema_final.sql` → Run

### 3. Инициализация данных

**Через переменную окружения:**
```bash
export DATABASE_URL="postgresql://user:password@host:5432/foundry_db"
python3 init_db.py --data master_data.xlsx
```

**Или напрямую:**
```bash
python3 init_db.py \
  --connection "postgresql://user:password@host:5432/foundry_db" \
  --data master_data.xlsx
```

**Проверка без записи (dry-run):**
```bash
python3 init_db.py --data master_data.xlsx --dry-run
```

## Структура данных

### Справочники

**Склады (warehouses)**
- Литейный цех (production)
- Склад отливок (storage)
- Склад готовой продукции (storage)
- Брак (defects)

**Пресс-формы (molds)**
- 15 форм (№1-15)
- Ресурс: 100,000 ударов
- Статус: active

**Сборки (assemblies)**
- Иволга кресло
- 4523
- Лестница
- Комплект каркаса
- Опора дивана
- Привод подъемный
- Лестница Габарит Т

**Детали (details)**
- 21 деталь
- 15 с пресс-формами, 6 покупных
- 18 требуют покраски
- Связи: mold_id, assembly_id (денормализованы)

**Машины (machines)**
- 2 литейных
- 4 зачистных
- 2 дробеструйных
- 2 фрезерных

### Параметры производительности

**machine_mold_params** (для отливки)
- cycle_duration_minutes: 5 мин
- loading_duration_minutes: 2 мин

**machine_detail_params** (для остальных операций)
Зачистка:
- quantity_per_cycle: 10 деталей
- cycle_duration: 15 мин
- loading_duration: 5 мин

Дробеструй:
- quantity_per_cycle: 20 деталей
- cycle_duration: 10 мин
- loading_duration: 3 мин

Фрезеровка:
- quantity_per_cycle: 5 деталей
- cycle_duration: 20 мин
- loading_duration: 5 мин

## Логика данных

### Денормализация

```sql
-- Деталь хранит связь с формой
details.mold_id → molds.id
details.qty_per_hit → сколько деталей за удар

-- Деталь хранит связь со сборкой
details.assembly_id → assemblies.id
details.qty_in_assembly → сколько нужно в сборку
```

### Покупные детали

Детали без `mold_id` = покупные (не отливаются):
- К12.05.032.3 (Заглушка)
- К07.02.205.5 (Ручка)
- К07.02.203.5 (Заглушка)
- К07.06.101-01 (Корпус)
- К07.06.102 (Крышка)
- К08.03.027 (Опора)

## Проверка

После инициализации проверь:

```sql
-- Количество записей
SELECT 'warehouses' as table_name, COUNT(*) as cnt FROM warehouses
UNION ALL
SELECT 'molds', COUNT(*) FROM molds
UNION ALL
SELECT 'assemblies', COUNT(*) FROM assemblies
UNION ALL
SELECT 'details', COUNT(*) FROM details
UNION ALL
SELECT 'machines', COUNT(*) FROM machines
UNION ALL
SELECT 'machine_mold_params', COUNT(*) FROM machine_mold_params
UNION ALL
SELECT 'machine_detail_params', COUNT(*) FROM machine_detail_params;

-- Детали по сборкам
SELECT a.name as assembly, COUNT(*) as detail_count
FROM details d
JOIN assemblies a ON d.assembly_id = a.id
GROUP BY a.name
ORDER BY detail_count DESC;

-- Детали с формами vs покупные
SELECT 
  CASE WHEN mold_id IS NULL THEN 'Покупные' ELSE 'С формой' END as type,
  COUNT(*) as cnt
FROM details
GROUP BY type;

-- Детали с покраской
SELECT COUNT(*) as paint_required
FROM details
WHERE requires_painting = true;
```

## Зависимости

```bash
pip install pandas openpyxl psycopg2-binary --break-system-packages
```

Или через requirements.txt:
```
pandas>=2.0.0
openpyxl>=3.1.0
psycopg2-binary>=2.9.0
```

## Troubleshooting

**Ошибка подключения:**
```
❌ Ошибка подключения к БД: connection to server failed
```
→ Проверь connection string и доступность БД

**Ошибка TRUNCATE CASCADE:**
```
❌ cannot truncate a table referenced in a foreign key constraint
```
→ Проверь что используешь `TRUNCATE ... RESTART IDENTITY CASCADE`

**Дубликаты ключей:**
```
❌ duplicate key value violates unique constraint
```
→ Сначала очисти таблицу: `TRUNCATE TABLE ... RESTART IDENTITY CASCADE`

## MVP ограничения

- Параметры производительности упрощены (одинаковые для всех деталей)
- Нет истории изменений справочников
- Нет валидации бизнес-правил (делается в коде планировщика)
- Покраска не учтена как отдельная машина (пока нет в оборудовании)
