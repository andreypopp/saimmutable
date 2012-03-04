"""

    sqlalchemyro -- alternative instrumentation machinery for read-only mappers
    ===========================================================================

"""

__all__ = ("mapper",)

import weakref
import types

from sqlalchemy import exc as sa_exc, event, util
from sqlalchemy.orm import mapperlib, _mapper_registry
from sqlalchemy.orm import instrumentation, state, attributes
from sqlalchemy.orm.util import _INSTRUMENTOR

class Mapper(mapperlib.Mapper):

    def _configure_class_instrumentation(self):
        manager = ReadOnlyClassManager(self.class_)

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

class FakedState(object):

    def __init__(self, instance, manager):
        self.obj = weakref.ref(instance)
        self.manager = manager

    _false_ro       = property(lambda self: False)
    _empty_set_ro   = property(lambda self: util.EMPTY_SET)
    _nop            = lambda self, *args, **kw: None

    modified        = _false_ro
    expired         = _false_ro
    deleted         = _false_ro

    unloaded        = _empty_set_ro
    load_options    = _empty_set_ro

    commit_all      = _nop
    modified_event  = _nop
    expire          = _nop

    def __hash__(self):
        return id(self)

class ReadOnlyClassManager(instrumentation.ClassManager):

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
        return FakedState

    def new_instance(self, state=None):
        instance = self.class_.__new__(self.class_)
        setattr(instance, self.STATE_ATTR,
                    state or self._ro_state_constructor(
                        instance, self))
        return instance

    def setup_instance(self, instance, state=None):
        setattr(instance, self.STATE_ATTR,
                    state or self._ro_state_constructor(instance, self))
