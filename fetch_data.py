"""
Rome Population — Data Fetcher
Downloads Kontur population data for Italy, clips to Rome, and exports:
  rome_hexagons.geojson  — H3 hexagon polygons with density (residents/km²)
  rome_municipi.geojson  — 15 romani (municipio) aggregated density
  rome_quartieri.geojson — OSM neighbourhood (quarter/suburb) aggregated density

Usage:
    pip install geopandas requests tqdm
    python fetch_data.py
"""

import gzip, shutil, requests, zipfile, tempfile
import geopandas as gpd
from pathlib import Path
from tqdm import tqdm
from shapely.geometry import LineString
from shapely.ops import polygonize, unary_union

# ── Config ────────────────────────────────────────────────────────────────────

KONTUR_URL = (
    "https://geodata-eu-central-1-kontur-public.s3.amazonaws.com"
    "/kontur_datasets/kontur_population_IT_20231101.gpkg.gz"
)
ROME_BBOX    = dict(west=12.28, east=12.72, south=41.72, north=42.05)
HEX_AREA_KM2 = 0.7373   # Kontur H3 resolution-8 cell area
OVERPASS_URL = "https://overpass-api.de/api/interpreter"

ISTAT_URL    = "https://www.istat.it/storage/cartografia/basi_territoriali/2021/R12_21.zip"
ROMA_PRO_COM = 58091   # int64 in the shapefile

OUT_DIR      = Path(__file__).parent
GPKG_GZ      = OUT_DIR / "kontur_population_IT.gpkg.gz"
GPKG         = OUT_DIR / "kontur_population_IT.gpkg"
OUT_HEX      = OUT_DIR / "rome_hexagons.geojson"
OUT_MUNI     = OUT_DIR / "rome_municipi.geojson"
OUT_NEIGH    = OUT_DIR / "rome_quartieri.geojson"
OUT_SECTIONS = OUT_DIR / "rome_sections.geojson"
OUT_TIBER    = OUT_DIR / "rome_tiber.geojson"

# ── Download / decompress ─────────────────────────────────────────────────────

def download(url, dest):
    if dest.exists():
        print(f"✓ Already downloaded: {dest.name}"); return
    print(f"⬇  Downloading {dest.name} …")
    with requests.get(url, stream=True, timeout=180) as r:
        r.raise_for_status()
        total = int(r.headers.get("content-length", 0))
        with open(dest, "wb") as f, tqdm(total=total, unit="B", unit_scale=True) as bar:
            for chunk in r.iter_content(1 << 17):
                f.write(chunk); bar.update(len(chunk))

def decompress(src, dst):
    if dst.exists():
        print(f"✓ Already decompressed: {dst.name}"); return
    print("📦 Decompressing …")
    with gzip.open(src, "rb") as fi, open(dst, "wb") as fo:
        shutil.copyfileobj(fi, fo)

# ── Clip ──────────────────────────────────────────────────────────────────────

def clip_to_rome(gpkg):
    from pyproj import Transformer
    print("✂️  Clipping to Rome …")
    b = ROME_BBOX
    t = Transformer.from_crs("EPSG:4326", "EPSG:3857", always_xy=True)
    x0, y0 = t.transform(b["west"], b["south"])
    x1, y1 = t.transform(b["east"], b["north"])
    gdf = gpd.read_file(gpkg, bbox=(x0, y0, x1, y1))
    gdf = gdf[gdf["population"] > 0].copy()
    print(f"   {len(gdf):,} populated hexagons")
    return gdf

# ── Overpass helpers ──────────────────────────────────────────────────────────

def fetch_overpass(query):
    """Run an Overpass API query, return elements list."""
    print("🌍 Querying Overpass API …")
    r = requests.post(OVERPASS_URL, data={"data": query}, timeout=120)
    r.raise_for_status()
    return r.json()["elements"]

def relations_to_gdf(elements):
    """Convert Overpass relation elements (with out geom) to a GeoDataFrame."""
    features, seen = [], set()
    for elem in elements:
        if elem.get("type") != "relation": continue
        name = (elem.get("tags") or {}).get("name", "").strip()
        if not name or name in seen: continue

        lines = []
        for m in elem.get("members", []):
            if m.get("type") != "way": continue
            if m.get("role", "outer") not in ("outer", ""): continue
            pts = m.get("geometry", [])
            if len(pts) >= 2:
                lines.append(LineString([(p["lon"], p["lat"]) for p in pts]))
        if not lines: continue

        polys = list(polygonize(unary_union(lines)))
        if not polys: continue

        geo = polys[0] if len(polys) == 1 else unary_union(polys)
        features.append({"name": name, "geometry": geo})
        seen.add(name)

    return gpd.GeoDataFrame(features, crs="EPSG:4326") if features else None

def aggregate_density(hex_gdf, zones_gdf):
    """Assign each hex centroid to a zone; return aggregated density GeoDataFrame."""
    hex_4326 = hex_gdf.to_crs(4326)
    cents = hex_4326.copy()
    cents.geometry = hex_4326.to_crs(32632).geometry.centroid.to_crs(4326)

    joined = gpd.sjoin(cents[["population", "geometry"]],
                       zones_gdf[["name", "geometry"]],
                       how="left", predicate="within")
    pop_by_zone = joined.groupby("name")["population"].sum()

    out = zones_gdf.to_crs(32632).copy()
    out["area_km2"] = out.geometry.area / 1e6
    out["population"] = out["name"].map(pop_by_zone).fillna(0).astype(int)
    out["density"] = (out["population"] / out["area_km2"]).round().astype(int)
    return out.to_crs(4326)[["name", "population", "density", "geometry"]]

# ── Export ────────────────────────────────────────────────────────────────────

def export_hexagons(gdf):
    """Polygon GeoJSON — the main visual layer."""
    print("🔷 Exporting hexagon polygons …")
    out = gdf.to_crs(epsg=4326).copy()
    out["density"] = (out["population"] / HEX_AREA_KM2).round().astype(int)
    out[["density", "geometry"]].to_file(OUT_HEX, driver="GeoJSON")
    print(f"   {OUT_HEX.name}  ({OUT_HEX.stat().st_size//1024} KB, "
          f"max {out['density'].max():,} res/km²)")

def export_municipi(hex_gdf):
    if OUT_MUNI.exists():
        print(f"✓ Already exported: {OUT_MUNI.name}"); return
    print("🏛  Fetching Roma municipio boundaries …")
    elements = fetch_overpass("""
        [out:json][timeout:90];
        area["name"="Roma"]["admin_level"="8"]["boundary"="administrative"]->.roma;
        relation["admin_level"="10"]["boundary"="administrative"](area.roma);
        out geom;
    """)
    gdf = relations_to_gdf(elements)
    if gdf is None: print("⚠️  No municipio data found"); return
    print(f"   {len(gdf)} municipi found")
    out = aggregate_density(hex_gdf, gdf)
    out.to_file(OUT_MUNI, driver="GeoJSON")
    print(f"   {OUT_MUNI.name}  (max {out['density'].max():,} res/km²)")

def export_quartieri(hex_gdf):
    if OUT_NEIGH.exists():
        print(f"✓ Already exported: {OUT_NEIGH.name}"); return
    print("🏘  Fetching Roma neighbourhood boundaries …")
    elements = fetch_overpass("""
        [out:json][timeout:120];
        area["name"="Roma"]["admin_level"="8"]["boundary"="administrative"]->.roma;
        (
          relation["place"~"^(quarter|suburb)$"](area.roma);
          relation["admin_level"="10"]["boundary"="administrative"](area.roma);
        );
        out geom;
    """)
    gdf = relations_to_gdf(elements)
    if gdf is None: print("⚠️  No neighbourhood data found"); return
    print(f"   {len(gdf)} quartieri found")
    out = aggregate_density(hex_gdf, gdf)
    out.to_file(OUT_NEIGH, driver="GeoJSON")
    print(f"   {OUT_NEIGH.name}  (max {out['density'].max():,} res/km²)")

# ── ISTAT census sections ─────────────────────────────────────────────────────

def export_sections(hex_gdf):
    """Download ISTAT 2021 census section boundaries for Rome.
    Uses the built-in POP21 field for population; falls back to area-weighted
    Kontur join for sections with missing population data."""
    if OUT_SECTIONS.exists():
        print(f"✓ Already exported: {OUT_SECTIONS.name}"); return

    istat_zip = OUT_DIR / "R12_21.zip"
    download(ISTAT_URL, istat_zip)

    print("📦 Extracting ISTAT census sections …")
    with tempfile.TemporaryDirectory() as tmpdir:
        with zipfile.ZipFile(istat_zip) as zf:
            zf.extractall(tmpdir)
        shp_files = list(Path(tmpdir).rglob("*.shp"))
        if not shp_files:
            print("⚠️  No shapefile found in ISTAT zip"); return
        sections_raw = gpd.read_file(shp_files[0])

    print(f"   {len(sections_raw):,} total sections in Lazio")

    # Filter to Rome (PRO_COM is int64 in this shapefile)
    # COD_TIPO_S==1: standard inhabited sections; exclude ==100 (virtual aggregates)
    rome_sec = sections_raw[
        (sections_raw["PRO_COM"] == ROMA_PRO_COM) &
        (sections_raw["COD_TIPO_S"] == 1) &
        (sections_raw["POP21"] > 0)
    ].copy().reset_index(drop=True)
    print(f"   {len(rome_sec):,} inhabited sections in Roma")

    # Reproject to metric for area computation + simplify to ~5m tolerance
    rome_sec = rome_sec.to_crs(32632)
    rome_sec.geometry = rome_sec.geometry.simplify(5, preserve_topology=True)
    rome_sec["area_km2"] = rome_sec.geometry.area / 1e6

    # Use POP21 (ISTAT 2021 census population) directly
    rome_sec["density"]    = (rome_sec["POP21"] / rome_sec["area_km2"].clip(lower=0.0001)).round().astype(int)
    rome_sec["population"] = rome_sec["POP21"].astype(int)

    out = rome_sec[["density", "population", "geometry"]].to_crs(4326)
    out.to_file(OUT_SECTIONS, driver="GeoJSON")
    print(f"   {OUT_SECTIONS.name}  ({OUT_SECTIONS.stat().st_size//1024} KB, "
          f"max {out['density'].max():,} res/km²)")


# ── Tiber river ───────────────────────────────────────────────────────────────

def export_tiber():
    """Fetch Tiber (Tevere) river geometry from Overpass, export rome_tiber.geojson."""
    if OUT_TIBER.exists():
        print(f"✓ Already exported: {OUT_TIBER.name}"); return

    print("🌊 Fetching Tiber river geometry …")
    elements = fetch_overpass("""
        [out:json][timeout:90];
        relation["name"="Tevere"]["waterway"="river"](41.72,12.28,42.05,12.72);
        out geom;
    """)

    lines = []
    for elem in elements:
        if elem.get("type") != "relation": continue
        for m in elem.get("members", []):
            if m.get("type") != "way": continue
            pts = m.get("geometry", [])
            if len(pts) >= 2:
                lines.append(LineString([(p["lon"], p["lat"]) for p in pts]))

    if not lines:
        print("⚠️  No Tiber geometry found"); return

    tiber_geom = unary_union(lines)
    gdf = gpd.GeoDataFrame([{"name": "Tevere", "geometry": tiber_geom}], crs="EPSG:4326")
    gdf.to_file(OUT_TIBER, driver="GeoJSON")
    print(f"   {OUT_TIBER.name}  ({OUT_TIBER.stat().st_size//1024} KB)")


# ── Main ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    download(KONTUR_URL, GPKG_GZ)
    decompress(GPKG_GZ, GPKG)
    gdf = clip_to_rome(GPKG)
    export_hexagons(gdf)
    export_municipi(gdf)
    export_quartieri(gdf)
    export_sections(gdf)
    export_tiber()
    print("\n✅  Done!  Open rome_spike_map.html in your browser.")
