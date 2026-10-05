# Dataset manifest

StreamForge replays one month of NYC TLC yellow-taxi trip records. The raw
files are not committed; `data/fetch.sh` downloads them and checks the
checksums below.

| File | Source URL | Downloaded | SHA-256 | Size |
|---|---|---|---|---|
| `yellow_tripdata_2026-03.parquet` | https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2026-03.parquet | 2026-10-04 | `d7af794a7ac06cbbb5afb4d1df9e86d84f2e878584de708ff2ffca4bd7d234d9` | 67,891,249 B |
| `taxi_zone_lookup.csv` | https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv | 2026-10-04 | `1a99e105092230f8620f301edcca7f80d3080642ff404d28ed957d3fa222c8ed` | 12,331 B |
| `taxi_zones.zip` (zone shapes, used only to build `web/src/zones.json`) | https://d37ci6vzurychx.cloudfront.net/misc/taxi_zones.zip | 2026-10-04 | `f6d711917bb4340f8f644d5366c51665489eb2d426dd1a4a55677721ae5adf17` | 1,022,574 B |

Publisher: NYC Taxi & Limousine Commission, [TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page).
The page publishes monthly Parquet files with about a two-month delay and
states that the data was not created by the TLC, which makes no
representations as to its accuracy. No explicit licence is stated on that
page; the files are distributed publicly through NYC Open Data.

## Row counts and cleaning (2026-03)

Counted by `internal/tlc` and cross-checked with pandas (identical counts and
identical fare and distance sums):

| | rows |
|---|---|
| rows in file | 3,952,451 |
| pickup time outside March 2026 | 19 |
| pickup zone not in 1..263 (264/265 are "unknown") | 6,249 |
| fare outside $0..$500 | 20,690 |
| distance outside 0..100 miles | 139 |
| **kept** | **3,925,354** |

Kept trips are sorted by pickup time (stable sort) and replayed in that
order. Pickup timestamps are naive local times in the file; StreamForge
treats them as UTC everywhere, consistently.

Benchmarks say which source they used. The synthetic generator
(`-source synthetic`, Zipf-skewed zones) is only used for throughput tests.
