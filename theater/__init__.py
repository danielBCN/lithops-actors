import inspect

import theater.actor
from theater.director import start, shutdown


def make_decorator(class_id=None):
    def class_decorator(cls):
        if inspect.isclass(cls):
            # check extra parameters (in the future)

            return theater.actor.make_role_class(cls, class_id)

        raise TypeError("The @theater.remote decorator must be applied to "
                        "a class.")
    return class_decorator


def remote(*args, **kwargs):
    if len(args) == 1 and len(kwargs) == 0 and callable(args[0]):
        # Case where the decorator does not have parameters
        return make_decorator()(args[0])

    # Parse the keyword arguments from the decorator.
    error_string = ("The @theater.remote decorator must be applied either "
                    "with no arguments and no parentheses, or it must be "
                    "applied using some of the arguments: "
                    "'class_id', "
                    "like in @theater.remote(class_id='userclassid').")
    assert len(args) == 0 and len(kwargs) > 0, error_string
    for key in kwargs:
        assert key in [
            "class_id",
        ], error_string

    # Handle arguments.
    class_id = kwargs.get("class_id")

    return make_decorator(class_id=class_id)


def role(actor_type, class_id=None):
    decor_class = make_decorator(class_id=class_id)(actor_type)
    return decor_class
