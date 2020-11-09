using System;
using System.Reflection;
using System.Text;
using Akka.Actor;
using Akka.Serialization;
using EventStore.Client;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Akka.Persistence.EventStore
{
    public class DefaultSnapshotEventAdapter : ISnapshotAdapter
    {
        private readonly JsonSerializerSettings _settings;
        private readonly NewtonSoftJsonSerializer _serializer;

        public DefaultSnapshotEventAdapter(Akka.Serialization.Serialization serialization)
        {
            _settings = new JsonSerializerSettings
            {
                DateTimeZoneHandling = DateTimeZoneHandling.Utc
            };
            _serializer = new NewtonSoftJsonSerializer(serialization.System);
        }

        public EventData Adapt(SnapshotMetadata snapshotMetadata, object snapshot)
        {
            var @event = snapshot;
            var metadata = JObject.Parse("{}");

            metadata[Constants.EventMetadata.PersistenceId] = snapshotMetadata.PersistenceId;
            metadata[Constants.EventMetadata.OccurredOn] = DateTimeOffset.Now;
            metadata[Constants.EventMetadata.SequenceNr] = snapshotMetadata.SequenceNr;
            metadata[Constants.EventMetadata.Timestamp] = snapshotMetadata.Timestamp;
            metadata[Constants.EventMetadata.JournalType] = Constants.JournalTypes.SnapshotJournal;

            var dataBytes = ToBytes(@event, metadata, out var type, out var isJson);

            var metadataString = JsonConvert.SerializeObject(metadata, _settings);
            var metadataBytes = Encoding.UTF8.GetBytes(metadataString);

            return new EventData(Uuid.NewUuid(), type, dataBytes, metadataBytes);
        }

        public SelectedSnapshot Adapt(ResolvedEvent resolvedEvent)
        {
            var eventData = resolvedEvent.Event;

            var metadataString = Encoding.UTF8.GetString(eventData.Metadata.ToArray());
            var metadata = JsonConvert.DeserializeObject<JObject>(metadataString, _settings);
            var stream = (string) metadata.SelectToken(Constants.EventMetadata.PersistenceId);
            var sequenceNr = (long) metadata.SelectToken(Constants.EventMetadata.SequenceNr);
            var ts = (string) metadata.SelectToken(Constants.EventMetadata.Timestamp);
             
            var timestamp = metadata.Value<DateTime>(Constants.EventMetadata.Timestamp);

            var @event = ToEvent(resolvedEvent.Event.Data.ToArray(), metadata);

            
            var snapshotMetadata = new SnapshotMetadata(stream, sequenceNr, timestamp);
            return new SelectedSnapshot(snapshotMetadata, @event);
        }

        protected virtual byte[] ToBytes(object @event, JObject metadata, out string type, out bool isJson)
        {
            var eventType = @event.GetType();
            isJson = true;
            type = eventType.Name.ToEventCase();
            var clrEventType = string.Concat(eventType.FullName, ", ", eventType.GetTypeInfo().Assembly.GetName().Name);
            metadata[Constants.EventMetadata.ClrEventType] = clrEventType;

            return _serializer.ToBinary(@event);
        }
        
        protected virtual object ToEvent(byte[] bytes, JObject metadata)
        {
            var eventTypeString = (string)metadata.SelectToken(Constants.EventMetadata.ClrEventType);
            var eventType = Type.GetType(eventTypeString, true, true);
            return _serializer.FromBinary(bytes, eventType);
        }
    }
}