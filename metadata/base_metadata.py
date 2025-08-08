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
"""Base classes for reading and building metadata."""

import abc
import json
import pathlib
import threading
import typing

from google.cloud import bigquery


class MetadataClient:
    """A client that reads pre-processed metadata from a JSON file."""

    def __init__(
        self,
        project_id: str,
        dataset_name: str,
        metadata_file: typing.Optional[str] = None,
    ) -> None:
        self.project_id = project_id
        self.dataset_name = dataset_name
        if metadata_file:
            self._metadata_file_name = metadata_file
        else:
            self._metadata_file_name = f"{self.project_id}__{self.dataset_name}.json"
        self._metadata: typing.Dict[str, typing.Any] = {}
        self._lock = threading.Lock()

    def get_metadata(self) -> typing.Dict[str, typing.Any]:
        """Loads metadata from the specified JSON file."""
        if self._metadata:
            return self._metadata

        with self._lock:
            if not self._metadata:
                metadata_path = pathlib.Path(self._metadata_file_name)
                if not metadata_path.exists():
                    raise FileNotFoundError(
                        f"Metadata file not found: {self._metadata_file_name}"
                    )
                self._metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
        return self._metadata


class BaseMetadataBuilder(MetadataClient, abc.ABC):
    """Abstract base class for building metadata from a source system."""

    def get_metadata(self) -> typing.Dict[str, typing.Any]:
        """Extracts metadata, building from source if the cache file doesn't exist."""
        try:
            return super().get_metadata()
        except FileNotFoundError:
            with self._lock:
                # Re-check in case another thread created the file while waiting for the lock.
                if not pathlib.Path(self._metadata_file_name).exists():
                    self._extract_metadata()
                    self._enhance_metadata()
                    pathlib.Path(self._metadata_file_name).write_text(
                        json.dumps(self._metadata, indent=2)
                    )
            return super().get_metadata()

    @abc.abstractmethod
    def _extract_metadata(self) -> None:
        """Extracts metadata from the source system and populates self._metadata."""
        raise NotImplementedError

    def _enhance_metadata(self) -> None:
        """Optionally enhances metadata with extra information from local files."""
        pass