
-- =========================================
-- Base de datos: bigdata
-- Tabla de resultados de Flink SQL
-- =========================================

CREATE TABLE IF NOT EXISTS department_counts (
    dept_id INTEGER PRIMARY KEY,
    emp_count BIGINT NOT NULL
);

-- =========================================
-- Tabla adicional para registrar empleados
-- (opcional para futuras prácticas)
-- =========================================

CREATE TABLE IF NOT EXISTS employees (
    emp_id INTEGER PRIMARY KEY,
    name VARCHAR(100),
    dept_id INTEGER
);
