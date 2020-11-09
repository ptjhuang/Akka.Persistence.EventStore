using Akka.Actor;

namespace Akka.Persistence.EventStore.Tests.Query
{
    internal class TestActor : UntypedPersistentActor
    {
        public static Props Props(string persistenceId) => Actor.Props.Create(() => new TestActor(persistenceId));

        public sealed class DeleteCommand
        {
            public DeleteCommand(long toSequenceNr)
            {
                ToSequenceNr = toSequenceNr;
            }

            public long ToSequenceNr { get; }
        }

        public TestActor(string persistenceId)
        {
            PersistenceId = persistenceId;
        }

        public override string PersistenceId { get; }

        protected override void OnRecover(object message)
        {
        }

        protected override void OnCommand(object message)
        {
            switch (message)
            {
                case DeleteCommand delete:
                    DeleteMessages(delete.ToSequenceNr);
                    var requester = Sender;
                    BecomeStacked(msg => msg.Match()
                        .With<DeleteMessagesSuccess>(_ =>
                        {
                            requester.Tell($"{delete.ToSequenceNr}-deleted");
                            UnbecomeStacked();
                        })
                        .Default(_ => requester.Tell($"{delete.ToSequenceNr}-delete failed")));
                    break;
                case string cmd:
                    var sender = Sender;
                    Persist(cmd, e => sender.Tell($"{e}-done"));
                    break;
            }
        }
    }
}