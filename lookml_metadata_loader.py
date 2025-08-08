# Copyright 2024 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Metadata builder for LookML source files."""

import pathlib
import typing

import lkml  # type: ignore
from google.cloud import bigquery

from ..base_metadata import BaseMetadataBuilder


class LookMLMetadataBuilder(BaseMetadataBuilder):
    """Builds metadata by parsing a LookML file."""

    def __init__(
        self,
        lookml_file_path: str,
        project_id: str,
        dataset_name: str,
        metadata_file: typing.Optional[str] = None,
    ) -> None:
        """
        Args:
            lookml_file_path (str): Path to the LookML (.lkml) file.
            project_id (str): GCP project id of BigQuery data.
            dataset_name (str): BigQuery dataset name.
            metadata_file (str, Optional): Path to the output metadata JSON file.
        """
        # This builder doesn't require a bq_client, so we pass None.
        super().__init__(None, project_id, dataset_name, metadata_file)
        self.lookml_file_path = pathlib.Path(lookml_file_path)

    def _extract_metadata(self) -> None:
        """Parses the LookML file and transforms it into the metadata format."""
        if not self.lookml_file_path.exists():
            raise FileNotFoundError(f"LookML file not found at {self.lookml_file_path}")

        with open(self.lookml_file_path, "r", encoding="utf-8") as f:
            lookml_model = lkml.load(f)

        processed_metadata = {}

        for view in lookml_model.get("views", []):
            # Extracts table name, removing backticks and schema/project prefixes
            table_name_raw = view.get("sql_table_name", view["name"])
            table_name = table_name_raw.replace("`", "").split(".")[-1]

            table_metadata = {
                "source_name": view["name"],
                "source_label": view.get("label", view["name"]),
                "description": view.get("description", ""),
                "columns": {},
            }

            dimensions = view.get("dimensions", [])
            measures = view.get("measures", [])

            for column in dimensions + measures:
                # Extracts field name, removing the Looker table substitution syntax
                col_name = column.get("sql", column["name"]).replace("${TABLE}.", "")
                table_metadata["columns"][col_name] = {
                    "field_name": col_name,
                    "field_type": column.get("type", "string"),
                    "field_label": column.get("label", col_name),
                    "description": column.get("description", ""),
                    "is_nullable": not str(column.get("primary_key", "no")).lower() == "yes",
                }
            processed_metadata[table_name] = table_metadata

        self._metadata = processed_metadata