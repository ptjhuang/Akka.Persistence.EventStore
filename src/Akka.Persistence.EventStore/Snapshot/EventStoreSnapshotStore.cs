using Akka.Event;
using Akka.Persistence.Snapshot;
using EventStore.Client;
using System;
using System.Linq;
using System.Threading.Tasks;

namespace Akka.Persistence.EventStore.Snapshot
{
    public class EventStoreSnapshotStore : SnapshotStore
    {
        private class SelectedSnapshotResult
        {
            public long EventNumber = long.MaxValue;
            public SelectedSnapshot Snapshot;

            public static SelectedSnapshotResult Empty => new SelectedSnapshotResult();
        }

        private EventStoreClient _conn;
        private ISnapshotAdapter _snapshotAdapter;
        private readonly EventStoreSnapshotSettings _settings;

        private readonly ILoggingAdapter _log;
        private readonly Akka.Serialization.Serialization _serialization;

        public EventStoreSnapshotStore()
        {
            _settings = EventStorePersistence.Get(Context.System).SnapshotStoreSettings;
            _log = Context.GetLogger();
            _serialization = Context.System.Serialization;
        }

        protected override void PreStart()
        {
            base.PreStart();
            var connectionString = _settings.ConnectionString;
            _conn = new EventStoreClient(EventStoreClientSettings.Create(connectionString));

            _snapshotAdapter = BuildDefaultSnapshotAdapter();
        }

        protected override async Task<SelectedSnapshot> LoadAsync(string persistenceId,
            SnapshotSelectionCriteria criteria)
        {
            if (criteria.Equals(SnapshotSelectionCriteria.None))
            {
                return null;
            }

            var streamName = GetStreamName(persistenceId);


            if (!criteria.Equals(SnapshotSelectionCriteria.Latest))
            {
                var result = await FindSnapshot(streamName, criteria.MaxSequenceNr, criteria.MaxTimeStamp);
                return result.Snapshot;
            }

            var slice = _conn.ReadStreamAsync(Direction.Backwards, streamName, StreamPosition.End, 1, resolveLinkTos: false);
            if (await slice.ReadState == ReadState.StreamNotFound) return null;
            
            var snapshot = await slice.FirstOrDefaultAsync();
            return _snapshotAdapter.Adapt(snapshot);
        }

        protected override async Task SaveAsync(SnapshotMetadata metadata, object snapshot)
        {
            var streamName = GetStreamName(metadata.PersistenceId);
            try
            {
                var writeResult = await _conn.AppendToStreamAsync(
                    streamName,
                    StreamState.Any,
                    new[] { _snapshotAdapter.Adapt(metadata, snapshot) }
                );
                _log.Debug(
                    "Snapshot for `{0}` committed at log position (commit: {1}, prepare: {2})",
                    metadata.PersistenceId,
                    writeResult.LogPosition.CommitPosition,
                    writeResult.LogPosition.PreparePosition
                );
            }
            catch (Exception e)
            {
                _log.Warning(
                    "Failed to make a snapshot for {0}, failed with message `{1}`",
                    metadata.PersistenceId,
                    e.Message
                );
            }
        }

        protected override async Task DeleteAsync(SnapshotMetadata metadata)
        {
            var streamName = GetStreamName(metadata.PersistenceId);
            var m = await _conn.GetStreamMetadataAsync(streamName);
            if (m.StreamDeleted)
            {
                return;
            }

            var timestamp = metadata.Timestamp != DateTime.MinValue ? metadata.Timestamp : default(DateTime?);

            var result = await FindSnapshot(streamName, metadata.SequenceNr, timestamp);

            if (result.Snapshot == null)
            {
                return;
            }

            await TruncateStream(streamName, StreamPosition.FromInt64(result.EventNumber + 1), m.Metadata);
        }

        

        protected override async Task DeleteAsync(string persistenceId, SnapshotSelectionCriteria criteria)
        {
            var streamName = GetStreamName(persistenceId);
            var m = await _conn.GetStreamMetadataAsync(streamName);
            
            if (criteria.Equals(SnapshotSelectionCriteria.Latest))
            {
                var @event = await _conn.ReadStreamAsync(Direction.Backwards, streamName, StreamPosition.End, 1, resolveLinkTos: false).FirstOrDefaultAsync();
                if (@event.Event != null)
                {
                    var highestEventPosition = @event.OriginalEventNumber;
                    await TruncateStream(streamName, highestEventPosition, m.Metadata);
                }
            }
            else if (!criteria.Equals(SnapshotSelectionCriteria.None))
            {
                var timestamp = criteria.MaxTimeStamp != DateTime.MinValue ? criteria.MaxTimeStamp : default(DateTime?);

                var result = await FindSnapshot(streamName, criteria.MaxSequenceNr, timestamp);

                if (result.Snapshot == null)
                {
                    return;
                }

                await TruncateStream(streamName, StreamPosition.FromInt64(result.EventNumber + 1), m.Metadata);
            }
        }

        private ISnapshotAdapter BuildDefaultSnapshotAdapter()
        {
            Func<DefaultSnapshotEventAdapter> getDefaultAdapter = () => new DefaultSnapshotEventAdapter(_serialization);

            if (_settings.Adapter.ToLowerInvariant() == "default")
            {
                return getDefaultAdapter();
            }
            else if (_settings.Adapter.ToLowerInvariant() == "legacy")
            {
                return new LegacySnapshotEventAdapter();
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

                ISnapshotAdapter journalAdapter = (adapterConstructor != null
                    ? adapterConstructor.Invoke(new object[] { _serialization })
                    : Activator.CreateInstance(journalAdapterType)) as ISnapshotAdapter;
                if (journalAdapter == null)
                {
                    _log.Error(
                        $"Unable to create instance of type [{journalAdapterType.AssemblyQualifiedName}] Adapter for EventStoreJournal. Do you have an empty constructor? Falling back to default.");
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

        private async Task<SelectedSnapshotResult> FindSnapshot(string streamName, long maxSequenceNr,
            DateTime? maxTimeStamp)
        {
            var result = await _conn
                .ReadStreamAsync(Direction.Backwards, streamName, StreamPosition.End, resolveLinkTos: false)
                .Select(e => new SelectedSnapshotResult
                {
                    EventNumber = e.OriginalEventNumber.ToInt64(),
                    Snapshot = _snapshotAdapter.Adapt(e)
                })
                .FirstOrDefaultAsync(s =>
                    (!maxTimeStamp.HasValue ||
                        s.Snapshot.Metadata.Timestamp <= maxTimeStamp.Value) &&
                    s.Snapshot.Metadata.SequenceNr <= maxSequenceNr);

            return result ?? SelectedSnapshotResult.Empty;
        }

        private string GetStreamName(string persistenceId)
        {
            return $"{_settings.Prefix}{persistenceId}";
        }


        private async Task TruncateStream(string streamName, StreamPosition truncateBefore, StreamMetadata original)
        {
            var streamMetadata = new StreamMetadata(
                            original.MaxCount,
                            original.MaxAge,
                            truncateBefore,
                            original.CacheControl,
                            original.Acl,
                            original.CustomMetadata);
            await _conn.SetStreamMetadataAsync(streamName, StreamState.Any, streamMetadata);
        }
    }
}