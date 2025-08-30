# src/ README

## 🗂️ Overview

The `src/` directory contains the core simulation logic for the Leap data generation pipeline.  
Each script here is responsible for generating or processing one component of the synthetic game telemetry.  
Outputs are written to JSON/CSV for inspection and/or directly loaded into DuckDB for use in dbt transformations.

---

## 📄 Module Guide

### `session_generator.py`

- Creates player sessions based on sign-on data.
- Assigns teams, session durations, and simulates session start/end times.
- Calls the heartbeat generator to create player movement paths during sessions.

### `heartbeat_generator.py`

- Simulates player position data (X, Y, Z) over time for each session.
- Uses one of four movement functions to generate realistic paths.
- Applies grid boundaries and collision avoidance between players.
- Outputs heartbeats every 30 seconds during the session.

### `transaction_generator.py`

- Generates in-game purchase events from sign-on data and product dimension.
- Models varying purchase behavior based on player type and session activity.
- Saves to `leap_raw.event_transaction`.

### `product_generator.py`

- Generates synthetic in-game products with prices and categories.
- Writes a static seed CSV (`dim_products.csv`) for dbt to use.

### `summarizer.py`

- Aggregates raw heartbeat and session event data into session-level facts.
- Calculates metrics such as kills, deaths, and total session length.
- Writes results to staging tables in DuckDB.

### `utils.py`

- Shared helper functions used across modules.
- Examples: UUID player ID generation, sign-on modeling, country assignment, constants for file paths.

### `loader.py`

- Handles DuckDB connections and data I/O.
- Functions for loading tables into DataFrames, writing DataFrames to tables, and clearing old data.

---

## 🎯 Movement Functions

Player movement paths in `heartbeat_generator.py` are powered by four different mathematical models.  
Each function produces a distinct motion pattern for player coordinates in 3D space:

1. **Lorentzian Motion**  
   Based on the [Lorentzian function](https://en.wikipedia.org/wiki/Lorentzian_function), producing sharp peaks and long tails in movement speed.

2. **Bézier Curves**  
   Uses [Bézier curves](https://en.wikipedia.org/wiki/B%C3%A9zier_curve) to create smooth, controllable paths between points.

3. **Lissajous Curves**  
   [Lissajous curves](https://en.wikipedia.org/wiki/Lissajous_curve) create looping, oscillatory paths based on sine and cosine functions.

4. **Perlin Noise**  
   Employs [Perlin noise](https://en.wikipedia.org/wiki/Perlin_noise) for natural, random-looking motion commonly used in computer graphics.

---

## 📦 Data Flow

1. **Sign-on data** → `session_generator` builds sessions.
2. **Sessions** → `heartbeat_generator` simulates positions.
3. **Transactions** → `transaction_generator` adds purchases.
4. **Products** → `product_generator` creates product dimension.
5. **Summaries** → `summarizer` aggregates metrics for analysis.
6. **Loader** → handles reading/writing all generated data to DuckDB.

---

## 🛠️ Developer Notes

- Movement models are modular — adding a new movement type requires creating a `src/movement/step/{name}.py` file and registering it.
- `GRID_BOUNDS` in `heartbeat_generator` controls the playable space.
- Collision avoidance ensures players stay at least 1 unit apart.
- All modules can be imported and run independently for testing.
