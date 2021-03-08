import inspect
import logging
import uuid

import actors
from actors.util.inspect import (extract_signature, is_class_method,
                                 is_function_or_method,
                                 is_static_method)

logger = logging.getLogger(__name__)


class MethodHandler(object):
    def __init__(self, actor_proxy, method_name):
        self._actor_proxy = actor_proxy  # TODO: make this a weak ref
        self._method_name = method_name
        self._with_future = False

    def __call__(self, *args, **kwargs):
        raise TypeError("Actor methods cannot be called directly. Instead "
                        f"of running 'object.{self._method_name}()', try "
                        f"'object.{self._method_name}.remote()'.")

    def remote(self, *args, **kwargs):
        # TODO: we could let the user define the action id
        # TODO: check args for self references
        logger.debug(f"Calling method '{self._method_name}'"
                     f" on actor '{self._actor_proxy}'")

        # FIXME: checking all args for proxies is slow
        refs = False
        args = list(args)
        new_args = []
        for arg in args:
            if isinstance(arg, ActorProxy):
                refs = True
                arg = arg._to_weak()
            new_args.append(arg)
        new_kwargs = {}
        for key, value in kwargs:
            if isinstance(value, ActorProxy):
                refs = True
                value = value._to_weak()
            new_kwargs[key] = value
        new_action = Action(self._actor_proxy._thtr_actor_key,
                            self._method_name,
                            args=tuple(new_args), kwargs=new_kwargs, refs=refs)
        actors.director.send_action(new_action)
        if self._with_future:
            return "This would be a Future"

    @property
    def future(self):
        class WithFuture(MethodHandler):
            def __init__(s):
                super().__init__(self._actor_proxy, self._method_name)
                s._with_future = True

        return WithFuture()


class RoleClassMethodMetadata(object):
    """Metadata of methods in a Role class."""
    _cache = {}

    def __init__(self):
        class_name = type(self).__name__
        raise TypeError(f"{class_name} can not be constructed directly, "
                        f"instead of running '{class_name}()', "
                        f"try '{class_name}.create()'")

    @classmethod
    def clear_cache(cls):
        cls._cache.clear()

    @classmethod
    def create(cls, modified_class, actor_creation_function_descriptor):
        # Try to create an instance from cache.
        cached_meta = cls._cache.get(actor_creation_function_descriptor)
        if cached_meta is not None:
            return cached_meta

        # Create an instance without __init__ called.
        self = cls.__new__(cls)

        actor_methods = inspect.getmembers(modified_class,
                                           is_function_or_method)
        self.methods = dict(actor_methods)
        self.signatures = {}
        for method_name, method in actor_methods:
            # Whether or not this method requires binding of its first
            # argument. For class and static methods, we do not want to bind
            # the first argument, but we do for instance methods
            is_bound = (is_class_method(method)
                        or is_static_method(modified_class, method_name))

            self.signatures[method_name] = extract_signature(
                method, ignore_first=not is_bound)

        cls._cache[actor_creation_function_descriptor] = self
        return self


class RoleDescriptor(object):
    def __init__(self, name, module):
        self.class_name = name
        self.class_module = module

    @staticmethod
    def from_class(target):
        return RoleDescriptor(target.__name__, target.__module__)


class RoleClassMetadata:
    """Metadata for a role class.
    Attributes:
        enriched_class: The original class that was replaced.
        role_creation_function_descriptor: The function descriptor for
            the actor creation task.
        class_id: The ID of this actor class.
        class_name: The name of this class.
        method_meta: The actor method metadata.
    """

    def __init__(self, enriched_class,
                 role_creation_function_descriptor, class_id):
        self.enriched_class = enriched_class
        self.actor_creation_function_descriptor = \
            role_creation_function_descriptor
        self.class_name = role_creation_function_descriptor.class_name
        self.class_id = class_id
        self.method_meta = RoleClassMethodMetadata.create(
            enriched_class, role_creation_function_descriptor)
        self.proxy_crafter = lambda actor_key: ActorProxy(
            actor_key,
            self.method_meta.signatures,
            self.class_name,
            self.class_id
        )


class RoleClass(object):
    """An actor role class.

    This class replaces decorated role classes and can be
    used to obtain proxies to actors of that role.
    """

    def __init__(cls, name, bases, attr):
        """Prevents users from directly inheriting from a RoleClass.
        This will be called when a class is defined with a RoleClass object
        as one of its base classes. To intentionally construct a RoleClass,
        use the '_thtr_from_enriched_class' class method.
        Raises:
            TypeError: Always.
        """
        for base in bases:
            if isinstance(base, RoleClass):
                raise TypeError(
                    f"Attempted to define subclass '{name}' of actor "
                    f"class '{base.__thtr_metadata__.class_name}'. "
                    "Inheriting from actor classes is "
                    "not currently supported.")

        # This shouldn't be reached because one of the base classes must be
        # an actor class if this was meant to be subclassed.
        assert False, "RoleClass.__init__ should not be called."

    def __call__(self, *args, **kwargs):
        """Prevents users from directly instantiating an ActorClass.
        This will be called instead of __init__ when 'ActorClass()' is executed
        because an is an object rather than a metaobject. To properly
        instantiated a remote actor, use 'ActorClass.remote()'.
        Raises:
            Exception: Always.
        """
        raise TypeError("Actors cannot be instantiated. "
                        "Obtain references to them instead. "
                        f"Instead of '{self.__thtr_metadata__.class_name}()', "
                        f"use '{self.__thtr_metadata__.class_name}"
                        f".with_key(key)'.")

    @classmethod
    def _thtr_from_enriched_class(cls, enriched_class, class_id):
        for attribute in [
            'with_key',
            # "_remote",
            '_thtr_from_modified_class',
        ]:
            if hasattr(enriched_class, attribute):
                logger.warning("Creating an actor from class "
                               f"{enriched_class.__name__} overwrites "
                               f"attribute {attribute} of that class")

        # The role class we are constructing inherits from the
        # original class so it retains all class properties.
        class DerivedRoleClass(cls, enriched_class):
            pass

        name = f"RoleClass({enriched_class.__name__})"
        DerivedRoleClass.__module__ = enriched_class.__module__
        DerivedRoleClass.__name__ = name
        DerivedRoleClass.__qualname__ = name
        # Construct the base object.
        self = DerivedRoleClass.__new__(DerivedRoleClass)
        # Role creation function descriptor.
        actor_creation_function_descriptor = RoleDescriptor.from_class(
            enriched_class.__thtr_actor_class__)

        self.__thtr_metadata__ = RoleClassMetadata(
            enriched_class, actor_creation_function_descriptor, class_id)

        return self

    def remote(self, *args, **kwargs):
        meta = self.__thtr_metadata__
        actor_key = meta.class_id + ':' + str(uuid.uuid4())

        proxy = ActorProxy(
            actor_key,
            meta.method_meta.signatures,
            meta.class_name,
            meta.class_id
        )
        weak_ref = proxy._to_weak()

        actors.director.new_actor(meta, weak_ref,
                                  args, kwargs)

        return proxy

    def for_key(self, actor_key):
        meta = self.__thtr_metadata__

        proxy = ActorProxy(
            actor_key,
            meta.method_meta.signatures,
            meta.class_name,
            meta.class_id
        )

        return proxy


class WeakRef(object):
    def __init__(self, actor_key, methods_signs, class_name, class_id):
        self._thtr_actor_key = actor_key
        self._thtr_method_signatures = methods_signs
        self._thtr_class_name = class_name
        self._thtr_class_id = class_id

    def build_proxy(self):
        return ActorProxy._from_weak(self)


class ActorProxy(object):
    """An actor proxy or handle.

    Fields are prefixed with _thtr_ to hide them and to avoid collisions.
    """

    def __init__(self, actor_key, methods_signs, class_name, class_id):
        self._thtr_actor_key = actor_key
        self._thtr_method_signatures = methods_signs
        self._thtr_class_name = class_name
        self._thtr_class_id = class_id

        for method_name in self._thtr_method_signatures.keys():
            # TODO: Python function descriptors to load/import classes when
            #  needed. When recreating proxies or sending them elsewhere.
            # function_descriptor = PythonFunctionDescriptor(
            #     module_name, method_name, class_name)
            # self._function_descriptor[
            #     method_name] = function_descriptor
            method = MethodHandler(
                self,
                method_name)
            setattr(self, method_name, method)

        setattr(self, 'pls_stop', self.__stop)

    def __stop(self):
        actors.director.send_stop(self._thtr_actor_key)

    def _to_weak(self):
        return WeakRef(self._thtr_actor_key, self._thtr_method_signatures,
                       self._thtr_class_name, self._thtr_class_id)

    @staticmethod
    def _from_weak(weak: WeakRef):
        return ActorProxy(weak._thtr_actor_key, weak._thtr_method_signatures,
                          weak._thtr_class_name, weak._thtr_class_id)

    def __eq__(self, other):
        if isinstance(other, ActorProxy):
            return self._thtr_actor_key == other._thtr_actor_key
        return False

    # Make tab completion work.
    def __dir__(self):
        return self._thtr_method_signatures.keys()

    def __repr__(self):
        return (f"Actor("
                f"{self._thtr_class_name}, "
                f"{self._actor_key})")

    @property
    def _actor_key(self):
        return self._thtr_actor_key


class Action(object):
    """Create a new representation of an action.

    If no *action_id* is given, a new one is generated.
    """

    def __init__(self, actor_key, method_name, action_id=None,
                 args=None, kwargs=None, refs=False):
        self.args = args or []
        self.kwargs = kwargs or {}
        self.action_id = action_id or uuid.uuid4().hex
        self.actor_key = actor_key
        self.method_name = method_name
        self.refs = refs

    def call(self, instance):
        if self.refs:
            args = list(self.args)
            new_args = []
            for arg in args:
                if isinstance(arg, WeakRef):
                    arg = ActorProxy._from_weak(arg)
                new_args.append(arg)
            self.args = tuple(new_args)
            new_kwargs = {}
            for key, value in self.kwargs:
                if isinstance(value, WeakRef):
                    value = ActorProxy._from_weak(value)
                new_kwargs[key] = value
            self.kwargs = new_kwargs
        """Run this action on the *instance*"""
        method = getattr(instance, self.method_name)
        return method(*self.args, **self.kwargs)

    def __repr__(self):
        return f"Action({self.actor_key}, {self.method_name}, {self.action_id})"


def enrich_class(cls):
    # check if cls is already an enriched class.
    if hasattr(cls, '__thtr_actor_class__'):
        return cls

    # Modify the class to have an additional method that will be used
    # stop the actor.
    class Class(cls):
        __thtr_actor_class__ = cls  # The original actor class

        def __thtr_stop_actor__(self):
            pass  # TODO: is it necessary here?

    Class.__module__ = cls.__module__
    Class.__name__ = cls.__name__

    if not is_function_or_method(getattr(Class, '__init__', None)):
        # Add __init__ if it does not exist.
        # Assign an __init__ function will avoid many checks later on.
        def __init__(self):
            pass

        Class.__init__ = __init__

    return Class


def make_role_class(cls, class_id):
    if class_id is None:
        class_id = 'lithops:' + cls.__name__
    Enriched = enrich_class(cls)
    return RoleClass._thtr_from_enriched_class(Enriched, class_id)
