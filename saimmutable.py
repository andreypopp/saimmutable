"""

    saimmutable -- instrumentation machinery for immutable data models
    ==================================================================

"""

__all__ = ("mapper",)

import weakref
import types

from sqlalchemy import exc as sa_exc, event, util
from sqlalchemy.orm import mapperlib, _mapper_registry, interfaces
from sqlalchemy.orm import instrumentation, state, attributes
from sqlalchemy.orm.util import _INSTRUMENTOR

class Mapper(mapperlib.Mapper):

    def _configure_class_instrumentation(self):
        manager = ImmutableClassManager(self.class_)

        if self.non_primary:
            if not manager or not manager.is_mapped:
                raise sa_exc.InvalidRequestError(
                    "Class %s has no primary mapper configured.  Configure "
                    "a primary mapper first before setting up a non primary "
                    "Mapper." % self.class_)
            self.class_manager = manager
            self._identity_class = manager.mapper._identity_class
            _mapper_registry[self] = True
            return

        if manager is not None:
            assert manager.class_ is self.class_
            if manager.is_mapped:
                raise sa_exc.ArgumentError(
                    "Class '%s' already has a primary mapper defined. "
                    "Use non_primary=True to "
                    "create a non primary Mapper.  clear_mappers() will "
                    "remove *all* current mappers from all classes." %
                    self.class_)
            #else:
                # a ClassManager may already exist as
                # ClassManager.instrument_attribute() creates
                # new managers for each subclass if they don't yet exist.

        _mapper_registry[self] = True

        self.dispatch.instrument_class(self, self.class_)

        self.class_manager = manager

        manager.mapper = self
        manager.deferred_scalar_loader = self._load_scalar_attributes

        # The remaining members can be added by any mapper,
        # e_name None or not.
        if manager.info.get(_INSTRUMENTOR, False):
            return

        event.listen(manager, 'first_init', mapperlib._event_on_first_init,
                raw=True)
        event.listen(manager, 'init', mapperlib._event_on_init, raw=True)
        event.listen(manager, 'resurrect', mapperlib._event_on_resurrect,
                raw=True)

        for key, method in util.iterate_attributes(self.class_):
            if isinstance(method, types.FunctionType):
                if hasattr(method, '__sa_reconstructor__'):
                    self._reconstructor = method
                    event.listen(manager, 'load', _event_on_load, raw=True)
                elif hasattr(method, '__sa_validators__'):
                    for name in method.__sa_validators__:
                        self.validators = self.validators.union({name : method})

        manager.info[_INSTRUMENTOR] = self

def mapper(*args, **kwargs):
    return Mapper(*args, **kwargs)

class StateProxy(object):
    """ Doesn't manage state but instead proxy to instance.__dict__"""

    def __init__(self, instance, manager):
        self.obj = weakref.ref(instance)
        self.manager = manager
        self.class_ = instance.__class__
        self.load_options = util.EMPTY_SET
        self.load_path = ()

    _false_ro       = property(lambda self: False)
    _empty_set_ro   = property(lambda self: util.EMPTY_SET)
    _nop            = lambda self, *args, **kw: None

    modified        = _false_ro
    expired         = _false_ro
    deleted         = _false_ro

    unloaded        = _empty_set_ro

    commit_all      = _nop
    modified_event  = _nop
    expire          = _nop
    reset           = _nop
    commit          = _nop

    @property
    def committed_state(self):
        return {}

    @property
    def callables(self):
        return {}

    @property
    def pending(self):
        return {}

    @property
    def dict(self):
        obj = self.obj()
        if obj is not None:
            return obj.__dict__
        else:
            return {}

    def detach(self):
        self.session_id = None

    def _is_really_none(self):
        return self.obj()

    def get_impl(self, key):
        return self.manager[key].impl

    def __hash__(self):
        return id(self)

    def __getstate__(self):
        d = {
            "instance": self.obj(),
            "class_": self.class_,
            "load_options": self.load_options,
            "load_path": self.load_path,
        }
        if self.load_path:
            d['load_path'] = interfaces.serialize_path(self.load_path)
        return d

    def __setstate__(self, state):
        if state["instance"] is not None:
            self.obj = weakref.ref(state["instance"])
        self.class_ = state["class_"]
        self.load_options = state["load_options"]
        self.load_path = state["load_path"]
        self.manager = instrumentation.manager_of_class(self.class_)
        if "load_path" in state:
            self.load_path = interfaces.deserialize_path(state["load_path"])

class ImmutableClassManager(instrumentation.ClassManager):

    @util.memoized_property
    def first_init(self):
        self.dispatch.first_init(self, self.class_)

    @property
    def _state_constructor(self):
        self.first_init
        return state.InstanceState

    @property
    def _ro_state_constructor(self):
        self.first_init
        return StateProxy

    def new_instance(self, state=None):
        instance = self.class_.__new__(self.class_)
        setattr(instance, self.STATE_ATTR,
            state or self._ro_state_constructor(instance, self))
        return instance

    def setup_instance(self, instance, state=None):
        setattr(instance, self.STATE_ATTR,
            state or self._ro_state_constructor(instance, self))
