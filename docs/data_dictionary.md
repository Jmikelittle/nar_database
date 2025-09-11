# NAR Database - Data Dictionary

## Overview
This data dictionary describes the fields available in the National Address Register (NAR) database. The NAR contains address and location information for buildings across Canada.

## Data Structure
- **Location**: Physical building (one record per building)
- **Address**: Building unit (one or more records per location)
- **Relationship**: One location can have multiple addresses (e.g., apartment building)

## Field Definitions

### Core Identifiers
| Field | Mapped To | Description | Example |
|-------|-----------|-------------|---------|
| `LOC_GUID` | `location_id` | Globally unique identifier for location | `{12345678-1234-1234-1234-123456789012}` |
| `ADDR_GUID` | `address_id` | Globally unique identifier for address | `{87654321-4321-4321-4321-210987654321}` |

### Address Components
| Field | Mapped To | Description | Example |
|-------|-----------|-------------|---------|
| `APT_NO_LABEL` | `unit_number` | Apartment or suite number | `101`, `Suite 5A` |
| `CIVIC_NO` | `street_number` | Civic number | `123` |
| `CIVIC_NO_SUFFIX` | `street_number_suffix` | Civic number suffix | `A`, `1/2` |
| `OFFICIAL_STREET_NAME` | `street_name` | Official street name | `Main`, `King` |
| `OFFICIAL_STREET_TYPE` | `street_type` | Official street designator | `ST`, `AVE`, `DR` |
| `OFFICIAL_STREET_DIR` | `street_direction` | Official street direction | `N`, `SE`, `NW` |

### Geographic Information
| Field | Mapped To | Description | Example |
|-------|-----------|-------------|---------|
| `PROV_CODE` | `province_code` | Province code | `10` (NL), `24` (QC), `35` (ON) |
| `CSD_CODE` | `csd_code` | Census subdivision code (2025) | `1001519` |
| `CSD_TYPE_ENG_CODE` | `csd_type_eng_code` | Census subdivision type code English | `CY` (City) |
| `CSD_TYPE_FRE_CODE` | `csd_type_fre_code` | Census subdivision type code French | `V` (Ville) |
| `CSD_ENG_NAME` | `city` | Census subdivision English name | `St. John's`, `Toronto` |
| `CSD_FRE_NAME` | `city_french` | Census subdivision French name | `Montréal` |

### Electoral and Economic Regions
| Field | Mapped To | Description | Example |
|-------|-----------|-------------|---------|
| `ER_CODE` | `economic_region_code` | Economic region code (2021) | `1010` |
| `ER_ENG_NAME` | `economic_region` | Economic region English name | `Avalon Peninsula` |
| `ER_FRE_NAME` | `economic_region_french` | Economic region French name | `Péninsule d'Avalon` |
| `FED_CODE` | `federal_district_code` | Federal electoral district code (2023) | `10001` |
| `FED_ENG_NAME` | `federal_district` | Federal electoral district English name | `Avalon` |
| `FED_FRE_NAME` | `federal_district_french` | Federal electoral district French name | `Avalon` |

### Mailing Address
| Field | Mapped To | Description | Example |
|-------|-----------|-------------|---------|
| `MAIL_STREET_NAME` | `mail_street_name` | Mailing street name | `Main` |
| `MAIL_STREET_TYPE` | `mail_street_type` | Mailing street designator | `ST`, `AVE` |
| `MAIL_STREET_DIR` | `mail_street_direction` | Mailing street direction | `N`, `E` |
| `MAIL_MUN_NAME` | `mail_city` | Mailing municipality name | `St. John's` |
| `MAIL_PROV_ABVN` | `mail_province` | Mailing province abbreviation | `NL`, `ON`, `BC` |
| `MAIL_POSTAL_CODE` | `postal_code` | Mailing postal code | `A1C1A1`, `M5V3A8` |

### Survey Coordinates (Dominion Land Survey)
| Field | Mapped To | Description | Example |
|-------|-----------|-------------|---------|
| `BG_DLS_LSD` | `dls_lsd` | Legal Subdivision | `1-16` |
| `BG_DLS_QTR` | `dls_quarter` | Quarter | `NE`, `SW` |
| `BG_DLS_SCTN` | `dls_section` | Section | `1-36` |
| `BG_DLS_TWNSHP` | `dls_township` | Township | `001-126` |
| `BG_DLS_RNG` | `dls_range` | Range | `01-34` |
| `BG_DLS_MRD` | `dls_meridian` | Meridian | `W4`, `W5`, `W6` |

### Spatial Coordinates
| Field | Mapped To | Description | Example |
|-------|-----------|-------------|---------|
| `BG_LATITUDE` | `latitude` | Latitude coordinate (EPSG:4326) | `47.5615` |
| `BG_LONGITUDE` | `longitude` | Longitude coordinate (EPSG:4326) | `-52.7126` |
| `BG_X` | `spatial_x` | X coordinate (EPSG:3347) | `714423.45` |
| `BG_Y` | `spatial_y` | Y coordinate (EPSG:3347) | `5264123.78` |

### Building Information
| Field | Mapped To | Description | Example |
|-------|-----------|-------------|---------|
| `BU_USE` | `building_use` | Building usage codes | `RES` (Residential) |
| `BU_N_CIVIC_ADD` | `additional_delivery` | Additional delivery information | `PO Box 432`, `RR2 Site19 Box42` |

## Province Codes Reference
| Code | Province/Territory |
|------|-------------------|
| `10` | Newfoundland and Labrador |
| `11` | Prince Edward Island |
| `12` | Nova Scotia |
| `13` | New Brunswick |
| `24` | Quebec |
| `35` | Ontario |
| `46` | Manitoba |
| `47` | Saskatchewan |
| `48` | Alberta |
| `59` | British Columbia |
| `60` | Yukon |
| `61` | Northwest Territories |
| `62` | Nunavut |

## Coordinate Systems
- **EPSG:4326**: WGS84 geographic coordinate system (latitude/longitude)
- **EPSG:3347**: NAD83 / Statistics Canada Lambert projection (X/Y coordinates)

## Data Quality Notes
- Postal codes are stored in 6-character format without spaces (e.g., `A1C1A1`)
- All text fields are cleaned and standardized during processing
- Records without both `street_name` and `postal_code` are filtered out
- Location files contain building-level data; Address files contain unit-level data

## Database Schema
The processed SQLite database uses simplified field names for easier querying:
- Complex field names are mapped to simpler equivalents
- All geographic and administrative codes are preserved
- Coordinates are stored as numeric values for spatial queries

## Usage Examples
```python
# Query by postal code
db.query_by_postal_code("A1C1A1")

# Query by city
db.query_by_city("St. John's")

# Query by street
db.query_by_street("Water Street")
```

## Data Source
- **Source**: Statistics Canada - National Address Register
- **URL**: https://doi.org/10.25318/46260002-eng
- **Dataset ID**: 46260002
- **Update Frequency**: Regular updates available from Statistics Canada

---
*This data dictionary is based on the official NAR documentation and reflects the structure of the processed database.*
