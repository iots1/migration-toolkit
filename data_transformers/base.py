"""
Base transformer class - Core data transformation logic.

This module provides the DataTransformer class which is used by
controllers to apply transformations.
"""
from __future__ import annotations  # Enable modern type hints
from typing import Optional

import pandas as pd
from data_transformers.registry import transform_batch


class DataTransformer:
    """
    Core data transformation class.

    Provides methods to transform pandas Series using
    registered transformers.

    Example:
        >>> transformer = DataTransformer()
        >>> result = transformer.apply_transform(df['column'], ['TRIM', 'UPPER'])
    """

    def apply_transform(
        self,
        series: pd.Series,
        transformer_names: list[str],
        params: Optional[dict] = None
    ) -> pd.Series:
        """
        Apply one or more transformers to a series.

        Args:
            series: pandas Series to transform
            transformer_names: List of transformer names
            params: Optional parameters for transformers

        Returns:
            Transformed pandas Series

        Example:
            >>> transformer = DataTransformer()
            >>> result = transformer.apply_transform(
            ...     df['phone'],
            ...     ['TRIM', 'FORMAT_PHONE'],
            ...     {'FORMAT_PHONE': {'country_code': '66'}}
            ... )
        """
        return transform_batch(series, transformer_names, params)

    def apply_transforms(
        self,
        df: pd.DataFrame,
        mappings: list[dict]
    ) -> pd.DataFrame:
        """
        Apply transformations to multiple columns.

        Args:
            df: pandas DataFrame to transform
            mappings: List of mapping dictionaries with format:
                {
                    "column": "column_name",
                    "transformers": ["TRIM", "UPPER"],
                    "params": {"TRANSFORMER_NAME": {...}}
                }

        Returns:
            Transformed pandas DataFrame

        Example:
            >>> mappings = [
            ...     {"column": "name", "transformers": ["TRIM", "UPPER"]},
            ...     {"column": "phone", "transformers": ["FORMAT_PHONE"]}
            ... ]
            >>> result = transformer.apply_transforms(df, mappings)
        """
        result = df.copy()

        for mapping in mappings:
            column = mapping.get("column")
            transformer_names = mapping.get("transformers", [])
            params = mapping.get("params", {})

            if column in result.columns and transformer_names:
                result[column] = self.apply_transform(
                    result[column],
                    transformer_names,
                    params
                )

        return result
