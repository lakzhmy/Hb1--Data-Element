"""Bucket Speckle elements by a numeric property and publish to a target project."""
#yey

import math
from collections import defaultdict

from pydantic import Field, SecretStr
from speckle_automate import (
    AutomateBase,
    AutomationContext,
    execute_automate_function,
)
from specklepy.api import operations
from specklepy.api.client import SpeckleClient
from specklepy.core.api.inputs.model_inputs import CreateModelInput
from specklepy.core.api.inputs.project_inputs import ProjectModelsFilter
from specklepy.core.api.inputs.version_inputs import CreateVersionInput
from specklepy.objects import Base
from specklepy.transports.server import ServerTransport

from flatten import flatten_base


class FunctionInputs(AutomateBase):
    """User-configurable inputs for the area bucketing function."""

    speckle_token: SecretStr = Field(
        title="Speckle Token",
        description="Personal access token with write access to the target project. "
        "The automation token is scoped to the source project only.",
    )
    target_project_id: str = Field(
        title="Target Project ID",
        description="The Speckle project ID where bucketed models will be published.",
    )
    target_model_id: str = Field(
        title="Target Model ID",
        description="Parent model ID in the target project. "
        "Sub-models for each bucket will be created under this model.",
    )
    bucket_size: float = Field(
        default=100.0,
        title="Bucket Size",
        description="Size of each area range bucket in model units.",
        gt=0,
    )
    property_name: str = Field(
        default="2D area",
        title="Property Name",
        description="Name of the numeric property to bucket by.",
    )


def get_property_value(element: Base, property_name: str) -> float | None:
    """Extract a numeric property value from a Speckle Base object."""
    # Direct attribute access (works for dynamic props like "2D area")
    value = getattr(element, property_name, None)
    if value is not None:
        try:
            return float(value)
        except (ValueError, TypeError):
            pass

    # Fallback: direct __dict__ lookup for names getattr can't resolve
    value = element.__dict__.get(property_name)
    if value is not None:
        try:
            return float(value)
        except (ValueError, TypeError):
            pass

    # Fallback: check inside 'parameters' sub-object
    parameters = getattr(element, "parameters", None)
    if parameters is not None:
        if isinstance(parameters, Base):
            value = getattr(parameters, property_name, None)
            if value is None:
                value = parameters.__dict__.get(property_name)
        elif isinstance(parameters, dict):
            value = parameters.get(property_name)
        if value is not None:
            try:
                return float(value)
            except (ValueError, TypeError):
                pass

    return None


def bucket_elements(
    elements_with_values: list[tuple[Base, float]],
    bucket_size: float,
) -> dict[int, list[Base]]:
    """Group elements into range buckets based on their numeric value."""
    buckets: dict[int, list[Base]] = defaultdict(list)
    for element, value in elements_with_values:
        bucket_index = int(math.floor(value / bucket_size))
        buckets[bucket_index].append(element)
    return dict(buckets)


def get_or_create_model(client, project_id: str, model_name: str):
    """Find an existing model by name or create a new one."""
    models_filter = ProjectModelsFilter(search=model_name)
    existing = client.model.get_models(project_id, models_filter=models_filter)

    for model in existing.items:
        if model.name == model_name:
            return model

    return client.model.create(
        CreateModelInput(
            name=model_name,
            description=f"Auto-generated bucket: {model_name}",
            project_id=project_id,
        )
    )


def publish_to_target_project(
    client,
    project_id: str,
    model_id: str,
    root_object: Base,
    version_message: str,
):
    """Send a Base object to a target project and create a version."""
    transport = ServerTransport(stream_id=project_id, client=client)
    object_id = operations.send(
        base=root_object,
        transports=[transport],
        use_default_cache=False,
    )
    return client.version.create(
        CreateVersionInput(
            object_id=object_id,
            model_id=model_id,
            project_id=project_id,
            message=version_message,
            source_application="SpeckleAutomate",
        )
    )


def automate_function(
    automate_context: AutomationContext,
    function_inputs: FunctionInputs,
) -> None:
    """Bucket elements by a numeric property and publish to a target project."""
    version_root_object = automate_context.receive_version()

    # Create a separate client for the target project using the personal token
    server_url = automate_context.automation_run_data.speckle_server_url
    target_client = SpeckleClient(host=server_url)
    target_client.authenticate_with_token(
        function_inputs.speckle_token.get_secret_value()
    )

    target_project_id = function_inputs.target_project_id
    parent_model = target_client.model.get(
        function_inputs.target_model_id, target_project_id
    )
    parent_name = parent_model.name

    # Flatten and extract elements with the target property
    elements_with_values: list[tuple[Base, float]] = []
    skipped = 0
    for obj in flatten_base(version_root_object):
        value = get_property_value(obj, function_inputs.property_name)
        if value is not None:
            elements_with_values.append((obj, value))
        else:
            skipped += 1

    count = len(elements_with_values)
    if count == 0:
        automate_context.mark_run_failed(
            f"No elements found with property '{function_inputs.property_name}'."
        )
        return

    # Bucket the elements
    buckets = bucket_elements(elements_with_values, function_inputs.bucket_size)
    bucket_size = function_inputs.bucket_size

    # Publish each bucket as a sub-model under the parent
    created = 0
    for bucket_index in sorted(buckets.keys()):
        elements = buckets[bucket_index]
        range_low = bucket_index * bucket_size
        range_high = range_low + bucket_size
        bucket_label = f"{range_low:.0f}-{range_high:.0f}"
        model_name = f"{parent_name}/{function_inputs.property_name} {bucket_label}"

        # Get or create sub-model
        model = get_or_create_model(target_client, target_project_id, model_name)

        # Build root object for this bucket
        root = Base()
        root["@elements"] = elements
        root["bucket_label"] = bucket_label
        root["element_count"] = len(elements)

        # Publish
        publish_to_target_project(
            client=target_client,
            project_id=target_project_id,
            model_id=model.id,
            root_object=root,
            version_message=(
                f"Bucket {bucket_label}: {len(elements)} elements "
                f"({function_inputs.property_name})"
            ),
        )

        # Annotate source objects
        automate_context.attach_info_to_objects(
            category=f"Bucket: {bucket_label}",
            affected_objects=elements,
            message=(
                f"{len(elements)} elements with "
                f"{function_inputs.property_name} in [{range_low:.0f}, {range_high:.0f})"
            ),
        )
        created += 1

    automate_context.mark_run_success(
        f"Published {count} elements across {created} buckets "
        f"to project {target_project_id}."
    )


if __name__ == "__main__":
    execute_automate_function(automate_function, FunctionInputs)
