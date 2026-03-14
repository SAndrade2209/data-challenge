from enum import Enum


class SourceFormat(Enum):
    CSV = "csv"
    JSON = "json"

    @classmethod
    def from_string(cls, value: str) -> "SourceFormat":
        """Converts a string to SourceFormat. Raises ValueError if not supported."""
        try:
            return cls(value.lower().strip())
        except ValueError:
            valid = [fmt.value for fmt in cls]
            raise ValueError(f"Unsupported format '{value}'. Must be one of: {valid}")
