-- ============================================================================
-- СХЕМА БД: Планировщик производства литейного цеха
-- ============================================================================
-- 
-- Назначение: учёт производства деталей через фазы обработки
-- Фазы: отливка → зачистка → дробеструй → фрезеровка → покраска (+ брак)
-- 
-- Логика планирования:
--   1. Внешние заказы на сборки → orders
--   2. Анализ потребности в деталях → detail_requirements (из orders или 1С)
--   3. Предварительный план (неделя/месяц/квартал/9мес) → tentative_production_plan
--   4. Твёрдый план на день → daily_production_plan
--   5. Факт выполнения → production_transactions
-- 
-- Особенности:
-- - Деталь на складе = (detail_id + phase)
-- - Брак учитывается как отдельная фаза
-- - Денормализованные связи: mold и assembly в таблице details
-- - Снапшоты состояния (машины, склад, металл) ежедневно из 1С
-- - Constraints и правила планирования хранятся в коде, не в БД
-- - Более краткосрочный план вычисляется из более долгосрочного
-- 
-- ============================================================================

DROP TABLE IF EXISTS production_transactions CASCADE;
DROP TABLE IF EXISTS daily_production_plan CASCADE;
DROP TABLE IF EXISTS tentative_production_plan CASCADE;
DROP TABLE IF EXISTS detail_requirements CASCADE;
DROP TABLE IF EXISTS orders CASCADE;
DROP TABLE IF EXISTS material_inventory_snapshots CASCADE;
DROP TABLE IF EXISTS inventory_snapshots CASCADE;
DROP TABLE IF EXISTS machine_state CASCADE;
DROP TABLE IF EXISTS machine_detail_params CASCADE;
DROP TABLE IF EXISTS machine_mold_params CASCADE;
DROP TABLE IF EXISTS machines CASCADE;
DROP TABLE IF EXISTS details CASCADE;
DROP TABLE IF EXISTS assemblies CASCADE;
DROP TABLE IF EXISTS molds CASCADE;
DROP TABLE IF EXISTS warehouses CASCADE;

-- ============================================================================
-- СПРАВОЧНИКИ
-- ============================================================================

CREATE TABLE warehouses (
    id SERIAL PRIMARY KEY,
    warehouse_name VARCHAR(200) UNIQUE NOT NULL,
    warehouse_type VARCHAR(50),
    notes TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_warehouses_name ON warehouses(warehouse_name);

CREATE TABLE molds (
    id SERIAL PRIMARY KEY,
    mold_number INT UNIQUE NOT NULL,
    name VARCHAR(100),
    install_date DATE,
    max_hits INT,
    status VARCHAR(20) DEFAULT 'active',
    notes TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT check_mold_status CHECK (status IN ('active', 'maintenance', 'worn_out', 'retired'))
);

CREATE INDEX idx_molds_number ON molds(mold_number);

CREATE TABLE assemblies (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) UNIQUE NOT NULL,
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Детали с денормализованными связями
CREATE TABLE details (
    id SERIAL PRIMARY KEY,
    nomenclature_code VARCHAR(100) UNIQUE NOT NULL,
    name TEXT NOT NULL,
    weight_kg DECIMAL(10, 4) NOT NULL,
    material_type VARCHAR(50) DEFAULT 'Алюминий',
    requires_painting BOOLEAN DEFAULT false,
    
    -- связь с формой (NULL для покупных)
    mold_id INT REFERENCES molds(id) ON DELETE SET NULL,
    qty_per_hit DECIMAL(10, 2),
    
    -- связь со сборкой
    assembly_id INT REFERENCES assemblies(id) ON DELETE SET NULL,
    qty_in_assembly INT,
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT check_qty_per_hit CHECK (qty_per_hit IS NULL OR qty_per_hit > 0),
    CONSTRAINT check_qty_in_assembly CHECK (qty_in_assembly IS NULL OR qty_in_assembly > 0)
);

CREATE INDEX idx_details_code ON details(nomenclature_code);
CREATE INDEX idx_details_mold ON details(mold_id);
CREATE INDEX idx_details_assembly ON details(assembly_id);

CREATE TABLE machines (
    id SERIAL PRIMARY KEY,
    machine_number INT UNIQUE NOT NULL,
    name VARCHAR(100) NOT NULL,
    output_phase VARCHAR(20) NOT NULL,
    status VARCHAR(20) DEFAULT 'active',
    notes TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT check_machine_status CHECK (status IN ('active', 'maintenance', 'idle', 'retired')),
    CONSTRAINT check_output_phase CHECK (output_phase IN ('отливка', 'зачистка', 'дробеструй', 'фрезеровка', 'покраска'))
);

CREATE INDEX idx_machines_number ON machines(machine_number);
CREATE INDEX idx_machines_phase ON machines(output_phase);

-- параметры производительности для отливки
CREATE TABLE machine_mold_params (
    id SERIAL PRIMARY KEY,
    machine_id INT NOT NULL REFERENCES machines(id) ON DELETE CASCADE,
    mold_id INT NOT NULL REFERENCES molds(id) ON DELETE CASCADE,
    cycle_duration_minutes INT NOT NULL,
    loading_duration_minutes INT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE(machine_id, mold_id)
);

CREATE INDEX idx_machine_mold_machine ON machine_mold_params(machine_id);
CREATE INDEX idx_machine_mold_mold ON machine_mold_params(mold_id);

-- параметры производительности для остальных фаз
CREATE TABLE machine_detail_params (
    id SERIAL PRIMARY KEY,
    machine_id INT NOT NULL REFERENCES machines(id) ON DELETE CASCADE,
    detail_id INT NOT NULL REFERENCES details(id) ON DELETE CASCADE,
    quantity_per_cycle INT NOT NULL,
    cycle_duration_minutes INT NOT NULL,
    loading_duration_minutes INT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE(machine_id, detail_id)
);

CREATE INDEX idx_machine_detail_machine ON machine_detail_params(machine_id);
CREATE INDEX idx_machine_detail_detail ON machine_detail_params(detail_id);

-- ============================================================================
-- СНАПШОТЫ СОСТОЯНИЯ (ежедневно из 1С)
-- ============================================================================

-- состояние машин
-- для литейных: config_params = {"mold_id": 5}
-- для остальных: {}
CREATE TABLE machine_state (
    id SERIAL PRIMARY KEY,
    state_date DATE NOT NULL,
    machine_id INT NOT NULL REFERENCES machines(id) ON DELETE CASCADE,
    status VARCHAR(20) NOT NULL,
    config_params JSONB,
    notes TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE(state_date, machine_id),
    CONSTRAINT check_machine_state_status CHECK (status IN ('working', 'changing_setup', 'maintenance', 'idle', 'out_of_order'))
);

CREATE INDEX idx_machine_state_date ON machine_state(state_date);
CREATE INDEX idx_machine_state_machine ON machine_state(machine_id);

-- остатки деталей на складе
-- ключ: дата + деталь + фаза + склад
CREATE TABLE inventory_snapshots (
    id SERIAL PRIMARY KEY,
    snapshot_date DATE NOT NULL,
    detail_id INT NOT NULL REFERENCES details(id) ON DELETE CASCADE,
    phase VARCHAR(20) NOT NULL,
    warehouse_id INT NOT NULL REFERENCES warehouses(id) ON DELETE CASCADE,
    quantity INT NOT NULL DEFAULT 0,
    imported_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source VARCHAR(20) DEFAULT '1C_export',
    
    UNIQUE(snapshot_date, detail_id, phase, warehouse_id),
    CONSTRAINT check_phase_snapshot CHECK (phase IN ('отливка', 'зачистка', 'дробеструй', 'фрезеровка', 'покраска', 'брак')),
    CONSTRAINT check_quantity_snapshot CHECK (quantity >= 0)
);

CREATE INDEX idx_inventory_snap_date ON inventory_snapshots(snapshot_date);
CREATE INDEX idx_inventory_snap_detail ON inventory_snapshots(detail_id);
CREATE INDEX idx_inventory_snap_phase ON inventory_snapshots(phase);
CREATE INDEX idx_inventory_snap_warehouse ON inventory_snapshots(warehouse_id);

-- остатки металла
CREATE TABLE material_inventory_snapshots (
    id SERIAL PRIMARY KEY,
    snapshot_date DATE NOT NULL,
    material_type VARCHAR(50) NOT NULL,
    quantity_kg DECIMAL(10, 2) NOT NULL DEFAULT 0,
    imported_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source VARCHAR(20) DEFAULT '1C_export',
    
    UNIQUE(snapshot_date, material_type),
    CONSTRAINT check_material_quantity_snap CHECK (quantity_kg >= 0)
);

CREATE INDEX idx_material_snap_date ON material_inventory_snapshots(snapshot_date);
CREATE INDEX idx_material_snap_type ON material_inventory_snapshots(material_type);

-- ============================================================================
-- ЗАКАЗЫ И ПОТРЕБНОСТИ
-- ============================================================================

-- внешние заказы на сборки
CREATE TABLE orders (
    id SERIAL PRIMARY KEY,
    assembly_id INT NOT NULL REFERENCES assemblies(id) ON DELETE CASCADE,
    due_date DATE NOT NULL,
    quantity INT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT check_order_quantity CHECK (quantity > 0)
);

CREATE INDEX idx_orders_due_date ON orders(due_date);
CREATE INDEX idx_orders_assembly ON orders(assembly_id);

-- потребности в деталях (на 9 месяцев)
-- источники: orders, 1С импорт, ручной ввод
CREATE TABLE detail_requirements (
    id SERIAL PRIMARY KEY,
    detail_id INT NOT NULL REFERENCES details(id) ON DELETE CASCADE,
    phase VARCHAR(20) NOT NULL,
    requirement_month DATE NOT NULL,
    required_quantity INT NOT NULL,
    source VARCHAR(50) NOT NULL,
    notes TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT check_requirement_phase CHECK (phase IN ('отливка', 'зачистка', 'дробеструй', 'фрезеровка', 'покраска')),
    CONSTRAINT check_required_quantity CHECK (required_quantity > 0),
    CONSTRAINT check_requirement_source CHECK (source IN ('from_orders', '1C_import', 'manual'))
);

CREATE INDEX idx_requirements_detail ON detail_requirements(detail_id);
CREATE INDEX idx_requirements_month ON detail_requirements(requirement_month);
CREATE INDEX idx_requirements_phase ON detail_requirements(phase);
CREATE INDEX idx_requirements_source ON detail_requirements(source);

-- ============================================================================
-- ПЛАНИРОВАНИЕ И ПРОИЗВОДСТВО
-- ============================================================================

-- предварительный план производства (неделя/месяц/квартал/9мес)
-- более краткосрочный вычисляется из более долгосрочного
CREATE TABLE tentative_production_plan (
    id SERIAL PRIMARY KEY,
    detail_id INT NOT NULL REFERENCES details(id) ON DELETE CASCADE,
    operation VARCHAR(20) NOT NULL,
    machine_id INT NOT NULL REFERENCES machines(id) ON DELETE CASCADE,
    start_date DATE NOT NULL,
    end_date DATE NOT NULL,
    quantity_planned INT NOT NULL,
    status VARCHAR(20) NOT NULL,
    notes TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT check_tentative_operation CHECK (operation IN ('отливка', 'зачистка', 'дробеструй', 'фрезеровка', 'покраска')),
    CONSTRAINT check_tentative_quantity CHECK (quantity_planned > 0),
    CONSTRAINT check_tentative_status CHECK (status IN ('heuristic', 'cached', 'emergency', 'obsolete')),
    CONSTRAINT check_date_range CHECK (end_date >= start_date)
);

CREATE INDEX idx_tentative_detail ON tentative_production_plan(detail_id);
CREATE INDEX idx_tentative_dates ON tentative_production_plan(start_date, end_date);
CREATE INDEX idx_tentative_status ON tentative_production_plan(status);
CREATE INDEX idx_tentative_machine ON tentative_production_plan(machine_id);

-- твёрдый план на день
CREATE TABLE daily_production_plan (
    id SERIAL PRIMARY KEY,
    plan_date DATE NOT NULL,
    detail_id INT NOT NULL REFERENCES details(id) ON DELETE CASCADE,
    operation VARCHAR(20) NOT NULL,
    quantity_planned INT NOT NULL,
    machine_id INT NOT NULL REFERENCES machines(id) ON DELETE CASCADE,
    shift_number INT DEFAULT 1,
    notes TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE(plan_date, detail_id, operation, machine_id),
    CONSTRAINT check_daily_operation CHECK (operation IN ('отливка', 'зачистка', 'дробеструй', 'фрезеровка', 'покраска')),
    CONSTRAINT check_daily_quantity CHECK (quantity_planned > 0)
);

CREATE INDEX idx_daily_date ON daily_production_plan(plan_date);
CREATE INDEX idx_daily_detail ON daily_production_plan(detail_id);
CREATE INDEX idx_daily_machine ON daily_production_plan(machine_id);

-- фактические производственные операции
CREATE TABLE production_transactions (
    id SERIAL PRIMARY KEY,
    transaction_date DATE NOT NULL,
    detail_id INT NOT NULL REFERENCES details(id) ON DELETE CASCADE,
    operation_type VARCHAR(20) NOT NULL,
    quantity INT NOT NULL,
    from_phase VARCHAR(20),
    to_phase VARCHAR(20) NOT NULL,
    machine_id INT REFERENCES machines(id) ON DELETE SET NULL,
    operator_name VARCHAR(100),
    shift_number INT DEFAULT 1,
    notes TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT check_operation_type CHECK (operation_type IN ('отливка', 'зачистка', 'дробеструй', 'фрезеровка', 'покраска', 'брак', 'отгрузка')),
    CONSTRAINT check_transaction_quantity CHECK (quantity > 0)
);

CREATE INDEX idx_transactions_date ON production_transactions(transaction_date);
CREATE INDEX idx_transactions_detail ON production_transactions(detail_id);
CREATE INDEX idx_transactions_operation ON production_transactions(operation_type);
CREATE INDEX idx_transactions_machine ON production_transactions(machine_id);
CREATE INDEX idx_transactions_to_phase ON production_transactions(to_phase);

-- ============================================================================
-- ПРИМЕЧАНИЯ
-- ============================================================================

-- Вычисляемые значения:
-- 
-- Износ формы:
--   SELECT SUM(pt.quantity / d.qty_per_hit) as total_hits
--   FROM production_transactions pt
--   JOIN details d ON pt.detail_id = d.id
--   WHERE d.mold_id = X AND pt.operation_type = 'отливка'
-- 
-- Расход металла за период:
--   SELECT SUM(d.weight_kg * pt.quantity)
--   FROM production_transactions pt
--   JOIN details d ON pt.detail_id = d.id
--   WHERE pt.operation_type = 'отливка'
--   AND pt.transaction_date BETWEEN start_date AND end_date
-- 
-- Текущий инвентарь (деталь + фаза):
--   SELECT quantity FROM inventory_snapshots
--   WHERE snapshot_date = latest_date
--   AND detail_id = X AND phase = 'отливка'
--
-- Планировщик при перезаписи плана на день:
--   DELETE FROM daily_production_plan WHERE plan_date = ?
--   INSERT INTO daily_production_plan ...

-- ============================================================================
-- КОНЕЦ СХЕМЫ
-- ============================================================================
