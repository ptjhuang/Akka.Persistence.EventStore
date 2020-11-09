using Akka.Actor;
using Akka.Dispatch;
using Akka.Event;
using Akka.Persistence.EventStore.Query;
using Akka.Persistence.Journal;
using Akka.Util.Internal;
using EventStore.Client;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Threading.Tasks;

namespace Akka.Persistence.EventStore.Journal
{
    public class EventStoreJournal : AsyncWriteJournal, IWithUnboundedStash
    {
        public IStash Stash { get; set; }

        private readonly EventStoreClient _conn;
        private IEventAdapter _eventAdapter;
        private readonly EventStoreJournalSettings _settings;
        private readonly ILoggingAdapter _log;
        private readonly Akka.Serialization.Serialization _serialization;

        public EventStoreJournal()
        {
            _settings = EventStorePersistence.Get(Context.System).JournalSettings;
            _log = Context.GetLogger();
            _serialization = Context.System.Serialization;

            var connectionString = _settings.ConnectionString;
            var connectionName = _settings.ConnectionName;

            var settings = EventStoreClientSettings.Create(connectionString);
            settings.ConnectionName = $"{connectionName}";
            _conn = new EventStoreClient(settings);

            Self.Tell(new Status.Success("Connected"));
        }

        protected override void PreStart()
        {
            base.PreStart();
            _eventAdapter = BuildDefaultJournalAdapter();
            BecomeStacked(AwaitingConnection);
        }

        protected override void PostStop()
        {
            base.PostStop();
            _conn?.Dispose();
        }

        private bool AwaitingConnection(object message)
        {
            return message.Match()
                          .With<Status.Success>(success =>
                          {
                              UnbecomeStacked();
                              Stash.UnstashAll();
                          })
                          .With<Status.Failure>(fail =>
                          {
                              _log.Error(fail.Cause, "Failure during {0} initialization.", Self);
                              Context.Stop(Self);
                          })
                          .Default(_ => Stash.Stash())
                          .WasHandled;
        }

        private IEventAdapter BuildDefaultJournalAdapter()
        {
            Func<DefaultEventAdapter> getDefaultAdapter = () => new DefaultEventAdapter(_serialization);

            if (_settings.Adapter.ToLowerInvariant() == "default")
            {
                return getDefaultAdapter();
            }
            else if (_settings.Adapter.ToLowerInvariant() == "legacy")
            {
                return new LegacyEventAdapter(_serialization);
            }

            try
            {
                var journalAdapterType = Type.GetType(_settings.Adapter);
                if (journalAdapterType == null)
                {
                    _log.Error(
                        $"Unable to find type [{_settings.Adapter}] Adapter for EventStoreJournal. Is the assembly referenced properly? Falling back to default");
                    return getDefaultAdapter();
                }

                var adapterConstructor = journalAdapterType.GetConstructor(new[] { typeof(Akka.Serialization.Serialization) });

                IEventAdapter journalAdapter = (adapterConstructor != null
                    ? adapterConstructor.Invoke(new object[] { _serialization })
                    : Activator.CreateInstance(journalAdapterType)) as IEventAdapter;

                if (journalAdapter == null)
                {
                    _log.Error(
                        $"Unable to create instance of type [{journalAdapterType.AssemblyQualifiedName}] Adapter for EventStoreJournal. Do you have an empty constructor, or one that takes in Akka.Serialization.Serialization? Falling back to default.");
                    return getDefaultAdapter();
                }

                return journalAdapter;
            }
            catch (Exception e)
            {
                _log.Error(e, "Error loading Adapter for EventStoreJournal. Falling back to default");
                return getDefaultAdapter();
            }
        }

        public override async Task<long> ReadHighestSequenceNrAsync(string persistenceId, long fromSequenceNr)
        {
            try
            {
                long sequence = 0;

                var result = _conn.ReadStreamAsync(Direction.Backwards, persistenceId, StreamPosition.End, 1, resolveLinkTos: false);
                if ((await result.ReadState) == ReadState.StreamNotFound) return sequence;

                var @event = await result.FirstOrDefaultAsync();

                if (@event.Event != null)
                {
                    var adapted = _eventAdapter.Adapt(@event);
                    sequence = adapted.SequenceNr;
                }
                else
                {
                    var metadata = await _conn.GetStreamMetadataAsync(persistenceId);
                    if (metadata.Metadata.TruncateBefore != null)
                    {
                        sequence = metadata.Metadata.TruncateBefore.Value.ToInt64();
                    }
                }

                return sequence;
            }
            catch (Exception e)
            {
                _log.Error(e, e.Message);
                throw;
            }
        }

        public override async Task ReplayMessagesAsync(
            IActorContext context,
            string persistenceId,
            long fromSequenceNr,
            long toSequenceNr,
            long max,
            Action<IPersistentRepresentation> recoveryCallback)
        {
            try
            {
                if (toSequenceNr < fromSequenceNr || max == 0) return;

                if (fromSequenceNr == toSequenceNr)
                {
                    max = 1;
                }
                if (max == long.MaxValue && toSequenceNr > fromSequenceNr)
                {
                    max = toSequenceNr - fromSequenceNr + 1;
                }
                if (toSequenceNr > fromSequenceNr && max == toSequenceNr)
                {
                    max = toSequenceNr - fromSequenceNr + 1;
                }

                var count = 0L;

                var start = fromSequenceNr <= 0
                        ? StreamPosition.Start
                        : StreamPosition.Start + (ulong)(fromSequenceNr - 1);

                await _conn.ReadStreamAsync(Direction.Forwards, persistenceId, start, resolveLinkTos: false)
                    .TakeWhile(_ => count < max)
                    .ForEachAsync(@event =>
                    {
                        var representation = _eventAdapter.Adapt(@event);
                        recoveryCallback(representation);
                        count++;
                    });
            }
            catch (Exception e)
            {
                _log.Error(e, "Error replaying messages for: {0}", persistenceId);
                throw;
            }
        }

        protected override async Task<IImmutableList<Exception>> WriteMessagesAsync(
            IEnumerable<AtomicWrite> atomicWrites)
        {
            var results = ImmutableList.Create<Exception>();
            foreach (var atomicWrite in atomicWrites)
            {
                var persistentMessages = (IImmutableList<IPersistentRepresentation>)atomicWrite.Payload;

                var persistenceId = atomicWrite.PersistenceId;


                var lowSequenceId = persistentMessages.Min(c => c.SequenceNr) - 2;

                try
                {
                    var events = persistentMessages
                                 .Select(persistentMessage => _eventAdapter.Adapt(persistentMessage)).ToArray();

                    var pendingWrite = new
                    {
                        StreamId = persistenceId,
                        ExpectedSequenceId = lowSequenceId,
                        EventData = events,
                        debugData = persistentMessages
                    };
                    var expectedVersion = pendingWrite.ExpectedSequenceId < 0
                        ? StreamRevision.None
                        : StreamRevision.FromInt64(pendingWrite.ExpectedSequenceId);
                    
                    var result = await _conn.AppendToStreamAsync(pendingWrite.StreamId, expectedVersion, pendingWrite.EventData);
                    results = results.Add(null);
                }
                catch (WrongExpectedVersionException)
                {
                    throw;
                }
                catch (Exception e)
                {
                    results = results.Add(TryUnwrapException(e));
                }
            }

            return results;
        }

        protected override async Task DeleteMessagesToAsync(string persistenceId, long toSequenceNr)
        {
            if (toSequenceNr == long.MaxValue)
            {
                var @event = await _conn.ReadStreamAsync(Direction.Backwards, persistenceId, StreamPosition.End, 1, resolveLinkTos: false).FirstOrDefaultAsync();
                if (@event.Event != null)
                {
                    var highestEventPosition = @event.OriginalEventNumber.ToUInt64();
                    await _conn.SetStreamMetadataAsync(persistenceId, StreamState.Any,
                        new StreamMetadata(truncateBefore: highestEventPosition + 1));
                }
            }
            else
            {
                await _conn.SetStreamMetadataAsync(persistenceId, StreamState.Any,
                    new StreamMetadata(truncateBefore: StreamPosition.Start + (ulong)toSequenceNr),
                    opt => opt.ThrowOnAppendFailure = true);
            }
        }

        protected override bool ReceivePluginInternal(object message)
        {
            return message.Match()
                .With<GetJournalConfig>(() =>
                {
                    Sender.Tell(new GetJournalConfigResult { Settings = _settings, EventAdapter = _eventAdapter });
                })
                .WasHandled;
        }
    }
}