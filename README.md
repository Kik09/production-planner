# Создание системы планирования производства литейного цеха

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
