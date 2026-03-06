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
        gt=50,
    )
    property_name: str = Field(
        default="2D area",
        title="Property Name",
        description="Name of the numeric property to bucket by.",
    )


def _try_float(value) -> float | None:
    """Try to convert a value to float, returning None on failure."""
    if value is None:
        return None
    # If value is itself a Base object, check for a 'value' attribute inside it
    if isinstance(value, Base):
        inner = getattr(value, "value", None)
        if inner is not None:
            try:
                return float(inner)
            except (ValueError, TypeError):
                return None
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


def _get_base_keys(obj: Base) -> list[str]:
    """Get all dynamic property keys from a Base object."""
    # Try get_member_names() first (SpecklePy API)
    if hasattr(obj, "get_member_names"):
        try:
            return list(obj.get_member_names())
        except Exception:
            pass
    # Fall back to __dict__
    return [k for k in obj.__dict__ if not k.startswith("_")]


def _get_base_value(obj: Base, key: str):
    """Get a value from a Base object, trying multiple access methods."""
    # Try bracket notation first (SpecklePy's preferred access)
    try:
        val = obj[key]
        if val is not None:
            return val
    except (KeyError, TypeError, Exception):
        pass
    # Try getattr
    val = getattr(obj, key, None)
    if val is not None:
        return val
    # Try __dict__
    return obj.__dict__.get(key)


def _search_in_object(obj, property_name: str, max_depth: int = 5) -> float | None:
    """Recursively search for a property inside a Base object, dict, or list."""
    if max_depth <= 0:
        return None

    if isinstance(obj, Base):
        # Try all access methods for the target property
        value = _get_base_value(obj, property_name)
        result = _try_float(value)
        if result is not None:
            return result
        # Recurse into child attributes
        for key in _get_base_keys(obj):
            if key in ("id", "speckle_type", "totalChildrenCount"):
                continue
            child = _get_base_value(obj, key)
            if isinstance(child, (Base, dict, list)):
                result = _search_in_object(child, property_name, max_depth - 1)
                if result is not None:
                    return result
    elif isinstance(obj, dict):
        value = obj.get(property_name)
        result = _try_float(value)
        if result is not None:
            return result
        for child in obj.values():
            if isinstance(child, (Base, dict, list)):
                result = _search_in_object(child, property_name, max_depth - 1)
                if result is not None:
                    return result
    elif isinstance(obj, list):
        for item in obj:
            if isinstance(item, (Base, dict, list)):
                result = _search_in_object(item, property_name, max_depth - 1)
                if result is not None:
                    return result

    return None


def get_property_value(element: Base, property_name: str) -> float | None:
    """Extract a numeric property value from a Speckle Base object."""
    # 1. Try all direct access methods
    value = _get_base_value(element, property_name)
    result = _try_float(value)
    if result is not None:
        return result

    # 2. Search inside 'parameters' and 'properties' recursively
    for container_name in ("parameters", "properties"):
        container = _get_base_value(element, container_name)
        if container is not None:
            result = _search_in_object(container, property_name)
            if result is not None:
                return result

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
        # Debug: collect detailed info to help diagnose
        all_objects = list(flatten_base(version_root_object))
        sample_props: list[str] = []
        shown_properties_detail = 0  # limit verbose output
        for obj in all_objects[:15]:
            keys = _get_base_keys(obj)
            line = f"  [{obj.speckle_type}] keys: {keys}"
            # Show detailed 'properties' content for first 3 objects that have it
            if shown_properties_detail < 3:
                properties_attr = _get_base_value(obj, "properties")
                if properties_attr is not None:
                    shown_properties_detail += 1
                    line += f"\n    -> properties type: {type(properties_attr).__name__}"
                    if isinstance(properties_attr, Base):
                        pk = _get_base_keys(properties_attr)
                        line += f"\n    -> properties keys: {pk}"
                        # Show one level deeper for each sub-key
                        for k in pk[:5]:
                            child = _get_base_value(properties_attr, k)
                            if isinstance(child, Base):
                                ck = _get_base_keys(child)
                                line += f"\n       -> .{k} keys: {ck}"
                            elif isinstance(child, dict):
                                line += f"\n       -> .{k} dict keys: {list(child.keys())[:10]}"
                            elif isinstance(child, list):
                                line += f"\n       -> .{k} list len={len(child)}"
                                if child and isinstance(child[0], Base):
                                    line += f" item[0] keys: {_get_base_keys(child[0])}"
                            else:
                                line += f"\n       -> .{k} = {repr(child)[:100]}"
                    elif isinstance(properties_attr, dict):
                        line += f"\n    -> properties dict keys: {list(properties_attr.keys())[:15]}"
                    elif isinstance(properties_attr, list):
                        line += f"\n    -> properties list len={len(properties_attr)}"
                        if properties_attr and isinstance(properties_attr[0], Base):
                            line += f" item[0] keys: {_get_base_keys(properties_attr[0])}"
                    else:
                        line += f"\n    -> properties repr: {repr(properties_attr)[:200]}"
            sample_props.append(line)

        # Count speckle_types
        type_counts: dict[str, int] = {}
        for obj in all_objects:
            t = obj.speckle_type or "None"
            type_counts[t] = type_counts.get(t, 0) + 1
        type_summary = ", ".join(
            f"{t}: {c}" for t, c in sorted(type_counts.items(), key=lambda x: -x[1])[:10]
        )

        debug_msg = (
            f"[v3] No elements found with property '{function_inputs.property_name}'.\n"
            f"Total flattened objects: {len(all_objects)}\n"
            f"Type counts: {type_summary}\n"
            f"Sample objects (first 15):\n" + "\n".join(sample_props)
        )
        automate_context.mark_run_failed(debug_msg)
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
