# tests/ README

## 🧪 Overview

The `tests/` directory contains unit tests implemented using `pytest` to validate the correctness and integrity of the Leap data generation pipeline components.

---

## 📂 Test Files

- `test_db.py`  
  Tests related to DuckDB database connectivity, table creation, and data read/write operations.

- `test_products.py`  
  Validates product generation logic including product attributes, CSV output format, and seed data correctness.

- `test_sessions.py`  
  Covers session generation including sign-on modeling, heartbeat simulation, movement patterns, and session metadata consistency.

- `test_transactions.py`  
  Tests transaction generation, purchase modeling based on player behavior, and integration with product data.

---

## 🛠️ Running Tests

From the project root, run:

```bash
pytest tests/
```

Tests use isolated in-memory DuckDB instances or temporary files where applicable, ensuring no side effects on production data.

---

## ⚙️ Developer Notes

- Tests verify data schemas, expected ranges, and logical constraints (e.g., no negative durations, unique IDs).
- Movement function outputs are tested for boundary conditions and collision avoidance.
- Test coverage can be extended as new modules or features are added.
- Continuous integration pipelines should run these tests on every push to maintain code quality.
