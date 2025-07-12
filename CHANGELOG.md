## 0.2.0 (unreleased)

- Updated `deltalake` to 0.27.0
- Changed `transaction_versions` method to `transaction_version`
- Fixed `to_polars` method excluding partitioning columns
- Dropped support for Ruby < 3.2

## 0.1.7 (2025-05-03)

- Updated `deltalake` to 0.26.0

## 0.1.6 (2025-03-12)

- Updated `deltalake` to 0.25.0

## 0.1.5 (2025-01-28)

- Updated `deltalake` to 0.24.0

## 0.1.4 (2025-01-02)

- Updated `deltalake` to 0.23.0

## 0.1.3 (2024-12-28)

- Updated `deltalake` to 0.22.3
- Added support for Ruby 3.4
- Added `rechunk` and `columns` options to `to_polars` method

## 0.1.2 (2024-12-03)

- Updated `deltalake` to 0.22.2
- Added `merge` method to `Table`
- Added `set_table_properties` method

## 0.1.1 (2024-11-22)

- Added support for constraints
- Added support for small file compaction
- Added support for Z Ordering
- Added `history`, `partitions`, `protocol`, `repair`, and `restore` methods to `Table`
- Added experimental `load_cdf` method to `Table`
- Fixed handling of unsigned integers
- Fixed error with timestamps

## 0.1.0 (2024-11-20)

- First release
