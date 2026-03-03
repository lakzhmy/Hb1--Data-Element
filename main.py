# """This module contains the function's business logic.

# Use the automation_context module to wrap your function in an Automate context helper.
# """

# from pydantic import Field, SecretStr
# from speckle_automate import (
#     AutomateBase,
#     AutomationContext,
#     execute_automate_function,
# )

# from flatten import flatten_base


# class FunctionInputs(AutomateBase):
#     """These are function author-defined values.

#     Automate will make sure to supply them matching the types specified here.
#     Please use the pydantic model schema to define your inputs:
#     https://docs.pydantic.dev/latest/usage/models/
#     """

#     # An example of how to use secret values.
#     whisper_message: SecretStr = Field(title="This is a secret message")
#     forbidden_speckle_type: str = Field(
#         title="Forbidden speckle type",
#         description=(
#             "If a object has the following speckle_type,"
#             " it will be marked with an error."
#         ),
#     )


# def automate_function(
#     automate_context: AutomationContext,
#     function_inputs: FunctionInputs,
# ) -> None:
#     """This is an example Speckle Automate function.

#     Args:
#         automate_context: A context-helper object that carries relevant information
#             about the runtime context of this function.
#             It gives access to the Speckle project data that triggered this run.
#             It also has convenient methods for attaching results to the Speckle model.
#         function_inputs: An instance object matching the defined schema.
#     """
#     # The context provides a convenient way to receive the triggering version.
#     version_root_object = automate_context.receive_version()

#     objects_with_forbidden_speckle_type = [
#         b
#         for b in flatten_base(version_root_object)
#         if b.speckle_type == function_inputs.forbidden_speckle_type
#     ]
#     count = len(objects_with_forbidden_speckle_type)

#     if count > 0:
#         # This is how a run is marked with a failure cause.
#         automate_context.attach_error_to_objects(
#             category="Forbidden speckle_type"
#             f" ({function_inputs.forbidden_speckle_type})",
#             affected_objects=objects_with_forbidden_speckle_type,
#             message="This project should not contain the type: "
#             f"{function_inputs.forbidden_speckle_type}",
#         )
#         automate_context.mark_run_failed(
#             "Automation failed: "
#             f"Found {count} object that have one of the forbidden speckle types: "
#             f"{function_inputs.forbidden_speckle_type}"
#         )

#         # Set the automation context view to the original model/version view
#         # to show the offending objects.
#         automate_context.set_context_view()

#     else:
#         automate_context.mark_run_success("No forbidden types found.")

#     # If the function generates file results, this is how it can be
#     # attached to the Speckle project/model
#     # automate_context.store_file_result("./report.pdf")


# def automate_function_without_inputs(automate_context: AutomationContext) -> None:
#     """A function example without inputs.

#     If your function does not need any input variables,
#      besides what the automation context provides,
#      the inputs argument can be omitted.
#     """
#     pass


# # make sure to call the function with the executor
# if __name__ == "__main__":
#     # NOTE: always pass in the automate function by its reference; do not invoke it!

#     # Pass in the function reference with the inputs schema to the executor.
#     execute_automate_function(automate_function, FunctionInputs)

#     # If the function has no arguments, the executor can handle it like so
#     # execute_automate_function(automate_function_without_inputs)


"""Filter Revit elements by type and publish to a target model."""

from pydantic import Field
from speckle_automate import (
    AutomateBase,
    AutomationContext,
    execute_automate_function,
)
from specklepy.objects import Base

from flatten import flatten_base


class FunctionInputs(AutomateBase):
    """User-configurable inputs for the element filter function."""

    element_type: str = Field(
        title="Element type",
        description="Elements whose 'type' property matches this value "
        "will be filtered and published to the target model.",
    )
    target_model_id: str = Field(
        title="Target model ID",
        description="The Speckle model ID to publish the filtered elements to. "
        "Find it in the model URL: /projects/.../models/<model-id>",
    )


def automate_function(
    automate_context: AutomationContext,
    function_inputs: FunctionInputs,
) -> None:
    """Filter elements by type and publish matches to a target model."""
    version_root_object = automate_context.receive_version()

    matching_elements = [
        element
        for element in flatten_base(version_root_object)
        if getattr(element, "type", None) == function_inputs.element_type
    ]

    if matching_elements:
        root = Base()
        root.elements = matching_elements

        automate_context.create_new_version_in_project(
            root_object=root,
            model_id=function_inputs.target_model_id,
            version_message=f"Filtered {len(matching_elements)} elements of type "
            f"'{function_inputs.element_type}'",
        )

        automate_context.attach_success_to_objects(
            category="Type Filter",
            affected_objects=matching_elements,
            message=f"Published to '{function_inputs.target_model_id}'",
        )
        automate_context.mark_run_success(
            f"Found and published {len(matching_elements)} elements of type "
            f"'{function_inputs.element_type}'"
        )
    else:
        automate_context.mark_run_failed(
            f"No elements found with type '{function_inputs.element_type}'"
        )


if __name__ == "__main__":
    execute_automate_function(automate_function, FunctionInputs)