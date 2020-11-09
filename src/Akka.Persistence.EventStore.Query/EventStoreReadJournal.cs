using Akka.Actor;
using Akka.Configuration;
using Akka.Persistence.EventStore.Journal;
using Akka.Persistence.Journal;
using Akka.Persistence.Query;
using Akka.Streams.Dsl;
using EventStore.Client;
using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Akka.Persistence.EventStore.Query
{
    /// <inheritdoc />
    public class EventStoreReadJournal : IReadJournal,
            IPersistenceIdsQuery,
            ICurrentPersistenceIdsQuery,
            IEventsByPersistenceIdQuery,
            ICurrentEventsByPersistenceIdQuery,
            IEventsByTagQuery,
            ICurrentEventsByTagQuery
    {
        /// <summary>
        /// HOCON identifier
        /// </summary>
        public const string Identifier = "akka.persistence.query.journal.eventstore";

        private readonly string _writeJournalPluginId;
        private readonly IActorRef _journalRef;
        private readonly Func<EventStoreClient> _esdbClientFactory;
        private readonly EventStoreJournalSettings _settings;
        private readonly IEventAdapter _eventAdapter;


        /// <inheritdoc />
        public EventStoreReadJournal(ExtendedActorSystem system, Config config)
        {
            _writeJournalPluginId = config.GetString("write-plugin");

            _journalRef = Persistence.Instance.Apply(system).JournalFor(_writeJournalPluginId);
            var result = _journalRef.Ask<GetJournalConfigResult>(new GetJournalConfig()).Result;
            _settings = result.Settings;
            _eventAdapter = result.EventAdapter;
            _esdbClientFactory = () => new EventStoreClient(EventStoreClientSettings.Create(_settings.ConnectionString));
        }

        /// <summary>
        /// Returns a default query configuration for akka persistence SQLite-based journals and snapshot stores.
        /// </summary>
        /// <returns></returns>
        public static Config DefaultConfiguration()
        {
            return ConfigurationFactory.FromResource<EventStoreReadJournal>(
                "Akka.Persistence.EventStore.Query.reference.conf");
        }

        /// <summary>
        /// Query all <see cref="T:Akka.Persistence.PersistentActor" /> identifiers, i.e. as defined by the
        /// `persistenceId` of the <see cref="T:Akka.Persistence.PersistentActor" />.
        /// 
        /// The stream is not completed when it reaches the end of the currently used `persistenceIds`,
        /// but it continues to push new `persistenceIds` when new persistent actors are created.
        /// Corresponding query that is completed when it reaches the end of the currently
        /// used `persistenceIds` is provided by <see cref="M:Akka.Persistence.Query.ICurrentPersistenceIdsQuery.CurrentPersistenceIds" />.
        ///
        /// *Please note*, to use this feature, you need to enable `$streams` built-in projection in EventStore server. Please refer to
        /// EventStore server documentation to find out how.
        /// </summary>
        public Source<string, NotUsed> PersistenceIds() => GetEventsByStreamId("$streams", null, null, true)
            .Select(env => env.PersistenceId)
            .Named("AllPersistenceIds");

        /// <summary>
        /// Same type of query as <see cref="PersistenceIds"/> but the stream
        /// is completed immediately when it reaches the end of the "result set". Persistent
        /// actors that are created after the query is completed are not included in the stream.
        /// </summary>
        public Source<string, NotUsed> CurrentPersistenceIds() => GetEventsByStreamId("$streams", null, null, false)
            .Select(env => env.PersistenceId)
            .Named("CurrentPersistenceIds");

        /// <summary>
        /// <see cref="EventsByPersistenceId"/> is used for retrieving events for a specific
        /// <see cref="PersistentActor"/> identified by <see cref="Eventsourced.PersistenceId"/>.
        /// <para>
        /// You can retrieve a subset of all events by specifying <paramref name="fromSequenceNr"/> and <paramref name="toSequenceNr"/>
        /// or use `0L` and <see cref="long.MaxValue"/> respectively to retrieve all events. Note that
        /// the corresponding sequence number of each event is provided in the
        /// <see cref="EventEnvelope"/>, which makes it possible to resume the
        /// stream at a later point from a given sequence number.
        /// </para>
        /// The returned event stream is ordered by sequence number, i.e. the same order as the
        /// <see cref="PersistentActor"/> persisted the events. The same prefix of stream elements (in same order)
        ///  are returned for multiple executions of the query, except for when events have been deleted.
        /// <para>
        /// The stream is not completed when it reaches the end of the currently stored events,
        /// but it continues to push new events when new events are persisted.
        /// Corresponding query that is completed when it reaches the end of the currently
        /// stored events is provided by <see cref="CurrentEventsByPersistenceId"/>.
        /// </para>
        /// The SQLite write journal is notifying the query side as soon as events are persisted, but for
        /// efficiency reasons the query side retrieves the events in batches that sometimes can
        /// be delayed up to the configured `refresh-interval`.
        /// <para></para>
        /// The stream is completed with failure if there is a failure in executing the query in the
        /// backend journal.
        /// </summary>
        public Source<EventEnvelope, NotUsed> EventsByPersistenceId(string persistenceId, long fromSequenceNr,
            long toSequenceNr)
        {
            return GetEventsByStreamId(persistenceId, fromSequenceNr, toSequenceNr, true)
                .MapMaterializedValue(_ => NotUsed.Instance)
                .Named("EventsByPersistenceId-" + persistenceId);
        }


        /// <summary>
        /// Same type of query as <see cref="EventsByPersistenceId"/> but the event stream
        /// is completed immediately when it reaches the end of the "result set". Events that are
        /// stored after the query is completed are not included in the event stream.
        /// </summary>
        public Source<EventEnvelope, NotUsed> CurrentEventsByPersistenceId(
            string persistenceId, long fromSequenceNr, long toSequenceNr
        )
        {
            return GetEventsByStreamId(persistenceId, fromSequenceNr, toSequenceNr, false)
                .MapMaterializedValue(_ => NotUsed.Instance)
                .Named("CurrentEventsByPersistenceId-" + persistenceId);
        }

        /// <summary>
        /// <see cref="EventsByTag"/> is used for retrieving events that were marked with
        /// a given tag, e.g. all events of an Aggregate Root type.
        /// <para></para>
        /// To tag events you create an <see cref="IEventAdapter"/> that wraps the events
        /// in a <see cref="Tagged"/> with the given `tags`.
        /// <para></para>
        /// You can use <see cref="NoOffset"/> to retrieve all events with a given tag or retrieve a subset of all
        /// events by specifying a <see cref="Sequence"/>. The `offset` corresponds to an ordered sequence number for
        /// the specific tag. Note that the corresponding offset of each event is provided in the
        /// <see cref="EventEnvelope"/>, which makes it possible to resume the
        /// stream at a later point from a given offset.
        /// <para></para>
        /// The `offset` is exclusive, i.e. the event with the exact same sequence number will not be included
        /// in the returned stream.This means that you can use the offset that is returned in <see cref="EventEnvelope"/>
        /// as the `offset` parameter in a subsequent query.
        /// <para></para>
        /// In addition to the <paramref name="offset"/> the <see cref="EventEnvelope"/> also provides `persistenceId` and `sequenceNr`
        /// for each event. The `sequenceNr` is the sequence number for the persistent actor with the
        /// `persistenceId` that persisted the event. The `persistenceId` + `sequenceNr` is an unique
        /// identifier for the event.
        /// <para></para>
        /// The returned event stream is ordered by the offset (tag sequence number), which corresponds
        /// to the same order as the write journal stored the events. The same stream elements (in same order)
        /// are returned for multiple executions of the query. Deleted events are not deleted from the
        /// tagged event stream.
        /// <para></para>
        /// The stream is not completed when it reaches the end of the currently stored events,
        /// but it continues to push new events when new events are persisted.
        /// Corresponding query that is completed when it reaches the end of the currently
        /// stored events is provided by <see cref="CurrentEventsByTag"/>.
        /// <para></para>
        /// The SQL write journal is notifying the query side as soon as tagged events are persisted, but for
        /// efficiency reasons the query side retrieves the events in batches that sometimes can
        /// be delayed up to the configured `refresh-interval`.
        /// <para></para>
        /// The stream is completed with failure if there is a failure in executing the query in the
        /// backend journal.
        /// </summary>
        public Source<EventEnvelope, NotUsed> EventsByTag(string tag, Offset offset = null)
        {
            offset = offset ?? NoOffset.Instance;
            switch (offset)
            {
                case Sequence seq:
                    return GetEventsByStreamId(tag, seq.Value + 1, null, true);
                case NoOffset _:
                    return GetEventsByStreamId(tag, null, null, true);
                default:
                    throw new ArgumentException($"EventStoreReadJournal does not support {offset.GetType().Name} offsets");
            }
        }

        /// <summary>
        /// Same type of query as <see cref="EventsByTag"/> but the event stream
        /// is completed immediately when it reaches the end of the "result set". Events that are
        /// stored after the query is completed are not included in the event stream.
        /// </summary>
        /// <param name="tag"></param>
        /// <param name="offset">Zero-based Akka index</param>
        public Source<EventEnvelope, NotUsed> CurrentEventsByTag(string tag, Offset offset = null)
        {
            offset = offset ?? NoOffset.Instance;
            switch (offset)
            {
                case Sequence seq:
                    return GetEventsByStreamId(tag, seq.Value + 1, null, false);
                case NoOffset _:
                    return GetEventsByStreamId(tag, null, null, false);
                default:
                    throw new ArgumentException($"EventStoreReadJournal does not support {offset.GetType().Name} offsets");
            }
        }


        /// <summary>
        /// GetEventsByStreamId
        /// </summary>
        /// <param name="streamId"></param>
        /// <param name="fromOffset">Inclusive start</param>
        /// <param name="toOffset">Exclusive offset</param>
        /// <param name="isLive"></param>
        /// <returns></returns>
        private Source<EventEnvelope, NotUsed> GetEventsByStreamId(string streamId, long? fromOffset, long? toOffset, bool isLive)
        {
            if (fromOffset == toOffset && fromOffset != null && toOffset != null) return Source.Empty<EventEnvelope>();

            var esdbClient = _esdbClientFactory();

            if (!isLive)
            {
                // Non-live subscription
                var startFrom = getStartPosFromInt64(fromOffset);
                var count = toOffset == null || toOffset == long.MaxValue ? long.MaxValue :
                    (toOffset.Value - (fromOffset == null ? 0 : fromOffset.Value));
                if (count == 0) return Source.Empty<EventEnvelope>();
                return Source
                    .FromTask(Task.Run(async () =>
                    {
                        var result = esdbClient.ReadStreamAsync(Direction.Forwards, streamId, startFrom, count, resolveLinkTos: true);
                        if ((await result.ReadState) == ReadState.StreamNotFound)
                            return AsyncEnumerable.Empty<ResolvedEvent>();
                        else
                            return result;
                    }))
                    .Select(ae => ae.AsStreamSource())
                    .ConcatMany(t => t)
                    .Select(ToEnvelope)
                    .Where(t => t.envelope != null)
                    .Select(t => t.envelope);
            }
            else
            {
                // LIVE subscription
                return FirehoseSource.Of<EventEnvelope>(async args =>
                {
                    var startFrom = getStartPosFromInt64(args.Start);
                    return startFrom == StreamPosition.Start
                        ? await esdbClient.SubscribeToStreamAsync(streamId,
                            eventAppeared,
                            resolveLinkTos: true,
                            subscriptionDropped)
                        : await esdbClient.SubscribeToStreamAsync(streamId, startFrom,
                            eventAppeared,
                            resolveLinkTos: true,
                            subscriptionDropped);

                    Task eventAppeared(StreamSubscription subscription, ResolvedEvent @event, CancellationToken token)
                    {
                        var (envelope, position) = ToEnvelope(@event);
                        if (envelope != null) args.EventAppeared((envelope, position));
                        return Task.CompletedTask;
                    }

                    void subscriptionDropped(StreamSubscription subscription, SubscriptionDroppedReason reason, Exception ex)
                    {
                        if (reason != SubscriptionDroppedReason.Disposed)
                            args.Broken(reason.ToString());
                    }

                })
                .AsBackpressureSource(fromOffset == null || fromOffset == 0 ? (long?)null : fromOffset - 1, autoRecover: false)
                .TakeUntil(env => ((Sequence)env.Offset).Value >= (toOffset ?? long.MaxValue) - 1, true);
            }

            static StreamPosition getStartPosFromInt64(long? start) =>
                start == null ? StreamPosition.Start : start == Int64.MaxValue ? StreamPosition.End : StreamPosition.FromInt64(start.Value);
        }

        private (EventEnvelope envelope, long position) ToEnvelope(ResolvedEvent @event)
        {
            var position = @event.OriginalEventNumber.ToInt64();
            var eventOffset = Offset.Sequence(position);
            var persistentRep = _eventAdapter.Adapt(@event);
            if (persistentRep == null) return default;
            var envelope = new EventEnvelope(eventOffset, persistentRep.PersistenceId, persistentRep.SequenceNr, persistentRep.Payload);
            return (envelope, position);
        }
    }
}