from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Optional


@dataclass
class ClassifiedListing:
    # Identity
    id: str                          # Site-native listing/ad ID
    source: str                      # Site name from sites.json "name" field

    # Classification
    manufacturer: str                # e.g. "Volkswagen", normalised to title case
    model: str                       # e.g. "Golf GTI", normalised to title case
    year: Optional[int]              # Registration/model year

    # Pricing
    price: Optional[float]           # Asking/listing price
    currency: str                    # ISO 4217, default "GBP"

    # Vehicle specifics
    mileage: Optional[int]           # Odometer reading
    mileage_unit: str                # "miles" or "km"
    condition: Optional[str]         # "new", "used", "nearly_new", etc.
    fuel_type: Optional[str]         # "Petrol", "Diesel", "Electric", etc.
    transmission: Optional[str]      # "Manual", "Automatic", "Semi-Automatic"
    colour: Optional[str]

    # Location and link
    location: Optional[str]          # Town/city of listing
    url: Optional[str]               # Direct link to the listing

    # Timing
    listed_date: Optional[str]       # ISO 8601 date "YYYY-MM-DD" when ad was posted
    harvested_at: str = field(       # UTC ISO 8601 timestamp set at parse time
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )

    # Passthrough for unmapped fields
    raw: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "id":           self.id,
            "source":       self.source,
            "manufacturer": self.manufacturer,
            "model":        self.model,
            "year":         self.year,
            "price":        self.price,
            "currency":     self.currency,
            "mileage":      self.mileage,
            "mileage_unit": self.mileage_unit,
            "condition":    self.condition,
            "fuel_type":    self.fuel_type,
            "transmission": self.transmission,
            "colour":       self.colour,
            "location":     self.location,
            "url":          self.url,
            "listed_date":  self.listed_date,
            "harvested_at": self.harvested_at,
            "raw":          self.raw,
        }

    @property
    def storage_path_parts(self) -> tuple[str, str, str]:
        """Returns (manufacturer_slug, model_slug, date_str) for path construction."""
        mfr  = self.manufacturer.lower().replace(" ", "_") or "unknown"
        mdl  = self.model.lower().replace(" ", "_") or "unknown"
        date = self.listed_date or self.harvested_at[:10]
        return mfr, mdl, date
