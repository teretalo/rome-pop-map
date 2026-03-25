# Roma Population Density Map

Interactive map of Rome's population density using ISTAT 2021 census section boundaries.
Toggle between a flat choropleth and 3D spikes, or switch to a municipio-level view.

## Quick start

### 1. Install Python dependencies
```bash
pip install geopandas requests tqdm pyproj
```

### 2. Fetch & process data (~80 MB download)
```bash
python fetch_data.py
```
Downloads the Kontur Italy population dataset and ISTAT 2021 census section boundaries,
clips to Rome, and writes the GeoJSON files used by the map.

### 3. Serve locally (required — browser blocks local file:// fetches)
```bash
python -m http.server 8080
# then open http://localhost:8080/rome_spike_map.html
```

## Data sources
- **ISTAT Sezioni di Censimento 2021** — census section boundaries + population (POP21)
  https://www.istat.it/notizia/basi-territoriali-e-variabili-censuarie/
  License: CC BY 4.0
- **Kontur Population** (2023-11-01) — H3 hex resolution 8, used for municipio aggregation
  https://www.kontur.io/portfolio/population-dataset/
  License: CC BY 4.0
- **OpenStreetMap / Overpass API** — municipio boundaries, Tiber river geometry
