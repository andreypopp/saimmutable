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
from sqlalchemy.orm import exc as orm_exc
from sqlalchemy.orm.util import _INSTRUMENTOR, state_str
from sqlalchemy.orm.interfaces import EXT_CONTINUE

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

    def _instance_processor(self, context, path, reduced_path, adapter,
                                polymorphic_from=None,
                                only_load_props=None, refresh_state=None,
                                polymorphic_discriminator=None):

        """Produce a mapper level row processor callable
           which processes rows into mapped instances."""

        # note that this method, most of which exists in a closure
        # called _instance(), resists being broken out, as
        # attempts to do so tend to add significant function
        # call overhead.  _instance() is the most
        # performance-critical section in the whole ORM.

        pk_cols = self.primary_key

        if polymorphic_from or refresh_state:
            polymorphic_on = None
        else:
            if polymorphic_discriminator is not None:
                polymorphic_on = polymorphic_discriminator
            else:
                polymorphic_on = self.polymorphic_on

        version_id_col = self.version_id_col

        if adapter:
            pk_cols = [adapter.columns[c] for c in pk_cols]
            if polymorphic_on is not None:
                polymorphic_on = adapter.columns[polymorphic_on]
            if version_id_col is not None:
                version_id_col = adapter.columns[version_id_col]

        identity_class = self._identity_class

        new_populators = []
        existing_populators = []
        eager_populators = []
        load_path = context.query._current_path + path

        def populate_state(state, dict_, row, isnew, only_load_props):
            if isnew:
                if context.propagate_options:
                    state.load_options = context.propagate_options
                if state.load_options:
                    state.load_path = load_path

            if not new_populators:
                self._populators(context, path, reduced_path, row, adapter,
                                new_populators,
                                existing_populators,
                                eager_populators
                )

            if isnew:
                populators = new_populators
            else:
                populators = existing_populators

            if only_load_props is None:
                for key, populator in populators:
                    populator(state, dict_, row)
            elif only_load_props:
                for key, populator in populators:
                    if key in only_load_props:
                        populator(state, dict_, row)

        session_identity_map = context.session.identity_map

        def _instance(row, result):
            identitykey = (
                identity_class,
                tuple([row[column] for column in pk_cols]))
            instance = session_identity_map.get(identitykey)
            if instance is None:
                instance = self.class_manager.new_instance()
            dict_ = attributes.instance_dict(instance)
            state = attributes.instance_state(instance)
            state.key = identitykey
            state.session_id = context.session.hash_key
            session_identity_map.add(state)
            populate_state(state, dict_, row, True, only_load_props)
            return instance
        return _instance

def _event_on_load(state, ctx):
    instrumenting_mapper = state.manager.info[_INSTRUMENTOR]
    if instrumenting_mapper._reconstructor:
        instrumenting_mapper._reconstructor(state.obj())

_none_set = frozenset([None])

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
