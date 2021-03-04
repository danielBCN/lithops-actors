import inspect


def is_function_or_method(obj):
    return inspect.isfunction(obj) or inspect.ismethod(obj)


def is_class_method(f):
    """Returns whether the given method is a class method."""
    return hasattr(f, "__self__") and f.__self__ is not None


def is_static_method(cls, f_name):
    for cls in inspect.getmro(cls):
        if f_name in cls.__dict__:
            return isinstance(cls.__dict__[f_name], staticmethod)
    return False


def extract_signature(func, ignore_first=False):
    """Extract the function signature from the function.

    :param func: The function whose signature should be extracted.
    :param ignore_first: True if the first argument should be ignored.
        Like in the case of a class method.
    :return: List of Parameter objects representing the function signature.
    """
    signature_parameters = list(inspect.signature(func).parameters.values())

    if ignore_first:
        if len(signature_parameters) == 0:
            raise ValueError("Methods must take a 'self' argument, but the "
                             f"method '{func.__name__}' does not have one.")
        signature_parameters = signature_parameters[1:]

    return signature_parameters
