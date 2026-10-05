#!/usr/bin/env python3
"""Builds web/src/data/zones.json from the TLC taxi zone shapefile.

The shapefile is in New York State Plane (EPSG:2263, feet), which is
already a flat projection, so coordinates are only scaled into an SVG
viewBox and simplified. Run once; the output is committed.

    pip install pyshp shapely
    python3 scripts/build_zones.py data/taxi_zones.zip data/taxi_zone_lookup.csv
"""
import csv, io, json, sys, zipfile
import shapefile
from shapely.geometry import shape
from shapely.ops import unary_union

zip_path, lookup_path = sys.argv[1], sys.argv[2]
z = zipfile.ZipFile(zip_path)
names = {n.rsplit(".", 1)[1].lower(): n for n in z.namelist() if n.lower().endswith((".shp", ".shx", ".dbf"))}
r = shapefile.Reader(shp=io.BytesIO(z.read(names["shp"])), shx=io.BytesIO(z.read(names["shx"])), dbf=io.BytesIO(z.read(names["dbf"])))
fields = [f[0] for f in r.fields[1:]]
lookup = {int(row["LocationID"]): row for row in csv.DictReader(open(lookup_path))}

geoms = {}
for sr in r.shapeRecords():
    rec = dict(zip(fields, sr.record))
    zid = int(rec["LocationID"])
    g = shape(sr.shape.__geo_interface__)
    geoms[zid] = unary_union([geoms[zid], g]) if zid in geoms else g

minx = min(g.bounds[0] for g in geoms.values()); miny = min(g.bounds[1] for g in geoms.values())
maxx = max(g.bounds[2] for g in geoms.values()); maxy = max(g.bounds[3] for g in geoms.values())
W = 1000.0
scale = W / (maxx - minx)
H = (maxy - miny) * scale

def path(g):
    polys = [g] if g.geom_type == "Polygon" else list(g.geoms)
    out = []
    for p in polys:
        p = p.simplify(150, preserve_topology=True)  # 150 ft
        if p.is_empty:
            continue
        for ring in [p.exterior] + list(p.interiors):
            pts = [((x - minx) * scale, (maxy - y) * scale) for x, y in ring.coords]
            out.append("M" + "L".join(f"{x:.1f},{y:.1f}" for x, y in pts) + "Z")
    return "".join(out)

zones = []
for zid in sorted(geoms):
    g = geoms[zid]
    c = g.representative_point()
    lk = lookup.get(zid, {})
    zones.append({
        "id": zid,
        "name": lk.get("Zone", ""),
        "borough": lk.get("Borough", ""),
        "d": path(g),
        "cx": round((c.x - minx) * scale, 1),
        "cy": round((maxy - c.y) * scale, 1),
    })
json.dump({"width": W, "height": round(H, 1), "zones": zones}, open("web/src/data/zones.json", "w"), separators=(",", ":"))
print(len(zones), "zones", f"{W}x{H:.0f}")
