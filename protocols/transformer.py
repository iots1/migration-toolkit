"""
Transformer and Validator protocols - Abstract interfaces for data transformations.

These protocols define the contract for:
- Transformers: Convert data from one format to another
- Validators: Validate data meets certain criteria

Benefits:
- Pluggable: Can add new transformers/validators without modifying core code
- Testable: Can mock for unit tests
- Type-safe: Static type checking
"""
from typing import Protocol, runtime_checkable, Any


@runtime_checkable
class Transformer(Protocol):
    """
    Protocol for data transformers.

    Transformers convert pandas Series from one format to another.
    Examples: TRIM, UPPER, BUDDHIST_TO_ISO, FORMAT_PHONE
    """

    @property
    def name(self) -> str:
        """Unique transformer name (e.g., 'TRIM', 'BUDDHIST_TO_ISO')."""
        ...

    @property
    def label(self) -> str:
        """Human-readable label for UI (e.g., 'Trim Whitespace')."""
        ...

    @property
    def description(self) -> str:
        """Description of what this transformer does."""
        ...

    @property
    def has_params(self) -> bool:
        """Whether this transformer accepts parameters."""
        ...

    def transform(self, series: Any, params: dict | None = None) -> Any:
        """
        Apply transformation to a pandas Series.

        Args:
            series: pandas Series to transform
            params: Optional parameters for transformation

        Returns:
            Transformed pandas Series

        Example:
            >>> transformer = TrimTransformer()
            >>> result = transformer.transform(df['column_name'])
        """
        ...


@runtime_checkable
class Validator(Protocol):
    """
    Protocol for data validators.

    Validators check if data meets certain criteria.
    Examples: NOT_NULL, THAI_ID, IS_EMAIL, MIN_LENGTH_13
    """

    @property
    def name(self) -> str:
        """Unique validator name (e.g., 'NOT_NULL', 'THAI_ID')."""
        ...

    @property
    def label(self) -> str:
        """Human-readable label for UI."""
        ...

    @property
    def description(self) -> str:
        """Description of what this validator checks."""
        ...

    def validate(self, series: Any, params: dict | None = None) -> dict:
        """
        Validate a pandas Series.

        Args:
            series: pandas Series to validate
            params: Optional parameters for validation

        Returns:
            dict with keys:
                - valid (bool): Whether validation passed
                - errors (list): List of error messages
                - invalid_count (int): Number of invalid values
                - invalid_indices (list): Indices of invalid values

        Example:
            >>> validator = ThaiIdValidator()
            >>> result = validator.validate(df['citizen_id'])
            >>> print(result['valid'])
            False
            >>> print(result['invalid_count'])
            5
        """
        ...


# Concrete implementations will be in transformers/ and validators/ packages
# (See Phase 5 for implementation)
