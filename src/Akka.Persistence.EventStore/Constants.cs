namespace Akka.Persistence.EventStore
{
    public static class Constants
    {
        public static class EventMetadata
        {
            public const string ClrEventType = "clrEventType";
            public const string PersistenceId = "persistenceId";
            public const string Manifest = "manifest";
            public const string OccurredOn = "occurredOn";
            /// <summary>
            /// <see cref="IPersistentRepresentation.Sender"/>
            /// </summary>
            public const string SenderPath = "senderPath";
            /// <summary>
            /// <see cref="IPersistentRepresentation.SequenceNr"/>
            /// </summary>
            public const string SequenceNr = "sequenceNr";
            /// <summary>
            /// <see cref="IPersistentRepresentation.WriterGuid"/>
            /// </summary>
            public const string WriterGuid = "writerGuid";
            /// <summary>
            /// <see cref="IPersistentRepresentation.Timestamp"/>
            /// </summary>
            public const string Timestamp = "timestamp";
            public const string JournalType = "journalType";
        }

        public static class JournalTypes
        {
            public const string WriteJournal = "WriteJournal";
            public const string SnapshotJournal = "SnapshotJournal";
        }
    }
}
