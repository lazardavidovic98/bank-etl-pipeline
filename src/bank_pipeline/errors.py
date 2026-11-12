"""Custom exceptions used by the pipeline."""


class PipelineError(Exception):
    """Base class for expected pipeline errors."""


class ConfigError(PipelineError):
    """Missing or invalid configuration."""


class ExtractError(PipelineError):
    """The dataset could not be downloaded or read."""


class SourceUnavailableError(ExtractError):
    """The API did not respond (or kept failing) after all retries."""


class InvalidDataError(PipelineError):
    """The payload does not have the expected structure."""


class LoadError(PipelineError):
    """Writing to the database failed."""


class AlreadyRunningError(PipelineError):
    """Another pipeline run is already in progress."""
