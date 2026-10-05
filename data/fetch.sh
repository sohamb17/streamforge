#!/usr/bin/env bash
# Downloads the dataset StreamForge replays and verifies it against the
# checksums recorded in MANIFEST.md.
set -euo pipefail
cd "$(dirname "$0")"
BASE=https://d37ci6vzurychx.cloudfront.net
fetch() {
  if [ ! -f "$2" ]; then echo "downloading $2"; curl -fSL --retry 3 -o "$2.part" "$1" && mv "$2.part" "$2"; fi
}
fetch "$BASE/trip-data/yellow_tripdata_2026-03.parquet" yellow_tripdata_2026-03.parquet
fetch "$BASE/misc/taxi_zone_lookup.csv" taxi_zone_lookup.csv
sha256sum -c <<'SUMS'
d7af794a7ac06cbbb5afb4d1df9e86d84f2e878584de708ff2ffca4bd7d234d9  yellow_tripdata_2026-03.parquet
1a99e105092230f8620f301edcca7f80d3080642ff404d28ed957d3fa222c8ed  taxi_zone_lookup.csv
SUMS
