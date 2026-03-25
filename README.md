# Roma Population Spike Map

Interactive 3D population density spike map of Rome using Kontur 2023 data.

## Quick start

### 1. Install Python dependencies
```bash
pip install geopandas requests tqdm
```

### 2. Fetch & process data (~20 MB download)
```bash
python fetch_data.py
```
This downloads the Kontur Italy H3 hexagon population dataset,
clips it to Rome's bounding box, and writes `rome_population.json`.

### 3. Serve locally (required — browser blocks local file:// fetches)
```bash
python -m http.server 8080
# then open http://localhost:8080/rome_spike_map.html
```

## Data source
- **Kontur Population** (2023-11-01) — H3 hex resolution 8, ~400 m cells
  https://www.kontur.io/portfolio/population-dataset/
- License: CC BY 4.0

## Notes on resolution
- Kontur (~400 m) is the easiest starting point and matches Milos Popovic's workflow.
- For higher resolution, consider **ISTAT Sezioni di Censimento** (Italian census
  sections, ~100–200 households each). Requires joining the shapefile with the
  population variable table from https://www.istat.it/notizia/dati-per-sezioni-di-censimento/
- Meta/Facebook High-Resolution Population (~30 m) is available at HDX but
  produces millions of points — needs downsampling before the web render.
