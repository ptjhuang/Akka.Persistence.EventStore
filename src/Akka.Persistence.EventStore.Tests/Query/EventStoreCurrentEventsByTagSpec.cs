using Akka.Actor;
using Akka.Configuration;
using Akka.Persistence.EventStore.Query;
using Akka.Persistence.Query;
using Akka.Persistence.TCK.Query;
using Akka.Streams;
using Akka.Streams.Dsl;
using Akka.Streams.TestKit;
using FluentAssertions;
using System;
using System.Collections.Immutable;
using System.Linq;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;
using static Akka.Persistence.Query.Offset;

namespace Akka.Persistence.EventStore.Tests.Query
{
    public class EventStoreCurrentEventsByTagSpec : CurrentEventsByTagSpec, IClassFixture<DatabaseFixture>, IAsyncLifetime
    {
        private readonly DatabaseFixture _databaseFixture;

        private static Config Config(DatabaseFixture databaseFixture)
        {
            return ConfigurationFactory.ParseString($@"
				akka.loglevel = INFO
                akka.persistence.journal.plugin = ""akka.persistence.journal.eventstore""
                akka.persistence.journal.eventstore {{
                    class = ""Akka.Persistence.EventStore.Journal.EventStoreJournal, Akka.Persistence.EventStore""
                    connection-string = ""{databaseFixture.ConnectionString}""
                    connection-name = ""{nameof(EventStoreCurrentEventsByPersistenceIdSpec)}""
                    read-batch-size = 500
                }}
                akka.test.single-expect-default = 10s").WithFallback(EventStoreReadJournal.DefaultConfiguration());
        }

        public EventStoreCurrentEventsByTagSpec(DatabaseFixture databaseFixture, ITestOutputHelper output) : 
                base(Config(databaseFixture), nameof(EventStoreCurrentEventsByTagSpec), output)
        {
            ReadJournal = Sys.ReadJournalFor<EventStoreReadJournal>(EventStoreReadJournal.Identifier);
            _databaseFixture = databaseFixture;
        }

        public async Task InitializeAsync()
        {
            await _databaseFixture.Restart();
        }

        public Task DisposeAsync()
        {
            return Task.CompletedTask;
        }

        [Fact]
        public override void ReadJournal_query_CurrentEventsByTag_should_find_events_from_offset_exclusive()
        {
            var queries = ReadJournal as ICurrentEventsByTagQuery;

            var a = Sys.ActorOf(Query.TestActor.Props("a"));
            var b = Sys.ActorOf(Query.TestActor.Props("b"));
            var c = Sys.ActorOf(Query.TestActor.Props("c"));

            a.Tell("hello");
            ExpectMsg("hello-done");
            a.Tell("a green apple");
            ExpectMsg("a green apple-done");
            b.Tell("a black car");
            ExpectMsg("a black car-done");
            a.Tell("something else");
            ExpectMsg("something else-done");
            a.Tell("a green banana");
            ExpectMsg("a green banana-done");
            b.Tell("a green leaf");
            ExpectMsg("a green leaf-done");
            c.Tell("a green cucumber");
            ExpectMsg("a green cucumber-done");

            var greenSrc1 = queries.CurrentEventsByTag("green", offset: NoOffset());
            var probe1 = greenSrc1.RunWith(this.SinkProbe<EventEnvelope>(), Materializer);
            probe1.Request(2);
            probe1.ExpectNext<EventEnvelope>(p => p.PersistenceId == "a" && p.SequenceNr == 2L && p.Event.Equals("a green apple"));
            var offs = probe1.ExpectNext<EventEnvelope>(p => p.PersistenceId == "a" && p.SequenceNr == 4L && p.Event.Equals("a green banana")).Offset;
            probe1.Cancel();

            var greenSrc = queries.CurrentEventsByTag("green", offset: offs);
            var probe2 = greenSrc.RunWith(this.SinkProbe<EventEnvelope>(), Materializer);
            probe2.Request(10);
            // note that banana is not included, since exclusive offset
            probe2.ExpectNext<EventEnvelope>(p => p.PersistenceId == "b" && p.SequenceNr == 2L && p.Event.Equals("a green leaf"));
            probe2.Cancel();
        }


        [Fact]
        public async Task ReadJournal_query_offset_exclusivity_should_be_correct()
        {
            var journal = PersistenceQuery.Get(Sys)
                .ReadJournalFor<EventStoreReadJournal>(EventStoreReadJournal.Identifier);
            var persistenceId = Guid.NewGuid();
            var actor = Sys.ActorOf(Props.Create<TestPersistentActor>(persistenceId));
            actor.Tell("cmd");
            ExpectMsg("cmd");

            var tag = "test-" + persistenceId.ToString("n");

            IImmutableList<EventEnvelope> round1 = await journal.CurrentEventsByTag(tag)
                .CompletionTimeout(GetTimeoutOrDefault(null))
                .RunWith(Sink.Seq<EventEnvelope>(), Sys.Materializer());
            round1.Should().HaveCount(1);

            var item1Offset = round1.First().Offset;
            round1.First().Offset.Should().BeOfType<Sequence>().And.Be(Offset.Sequence(0));

            var round2 = await journal.CurrentEventsByTag(tag, item1Offset)
                .CompletionTimeout(GetTimeoutOrDefault(null))
                .RunWith(Sink.Seq<EventEnvelope>(), Sys.Materializer());
            round2.Should().BeEmpty();

            actor.Tell("cmd2");
            ExpectMsg("cmd2");

            var round3 = await journal.CurrentEventsByTag(tag, item1Offset)
                .CompletionTimeout(GetTimeoutOrDefault(null))
                .RunWith(Sink.Seq<EventEnvelope>(), Sys.Materializer());
            round3.Should().HaveCount(1);
        }

        private class TestPersistentActor : UntypedPersistentActor
        {
            public override string PersistenceId { get; }

            public TestPersistentActor(Guid id) => PersistenceId = "test-" + id.ToString("n");

            protected override void OnCommand(object message)
            {
                Persist(new MyEvent(), evt => Sender.Tell(message));
            }

            protected override void OnRecover(object message)
            { }
        }

        private class MyEvent { }


        [Fact]
        public override void ReadJournal_query_CurrentEventsByTag_should_find_existing_events()
        {
            var queries = ReadJournal as ICurrentEventsByTagQuery;
            var a = Sys.ActorOf(Query.TestActor.Props("a"));
            var b = Sys.ActorOf(Query.TestActor.Props("b"));

            a.Tell("hello");
            ExpectMsg("hello-done");
            a.Tell("a green apple");
            ExpectMsg("a green apple-done");
            b.Tell("a black car");
            ExpectMsg("a black car-done");
            a.Tell("something else");
            ExpectMsg("something else-done");
            a.Tell("a green banana");
            ExpectMsg("a green banana-done");
            b.Tell("a green leaf");
            ExpectMsg("a green leaf-done");

            AwaitAssert(() =>
            {
                var greenSrc = queries.CurrentEventsByTag("green", offset: NoOffset());
                var probe = greenSrc.RunWith(this.SinkProbe<EventEnvelope>(), Materializer);
                probe.Request(2);
                probe.ExpectNext<EventEnvelope>(p => p.PersistenceId == "a" && p.SequenceNr == 2L && p.Event.Equals("a green apple"));
                probe.ExpectNext<EventEnvelope>(p => p.PersistenceId == "a" && p.SequenceNr == 4L && p.Event.Equals("a green banana"));
                probe.ExpectNoMsg(TimeSpan.FromMilliseconds(500));
                probe.Request(2);
                probe.ExpectNext<EventEnvelope>(p => p.PersistenceId == "b" && p.SequenceNr == 2L && p.Event.Equals("a green leaf"));
                probe.ExpectComplete();
            });


            var blackSrc = queries.CurrentEventsByTag("black", offset: NoOffset());
            var probe2 = blackSrc.RunWith(this.SinkProbe<EventEnvelope>(), Materializer);
            probe2.Request(5);
            probe2.ExpectNext<EventEnvelope>(p => p.PersistenceId == "b" && p.SequenceNr == 1L && p.Event.Equals("a black car"));
            probe2.ExpectComplete();

            var appleSrc = queries.CurrentEventsByTag("apple", offset: NoOffset());
            var probe3 = appleSrc.RunWith(this.SinkProbe<EventEnvelope>(), Materializer);
            probe3.Request(5);
            probe3.ExpectNext<EventEnvelope>(p => p.PersistenceId == "a" && p.SequenceNr == 2L && p.Event.Equals("a green apple"));
            probe3.ExpectComplete();
        }


        //[Fact]
        //public virtual void ReadJournal_query_CurrentEventsByTag_should_see_all_1500_events()
        //{
        //    var queries = ReadJournal as ICurrentEventsByTagQuery;
        //    var a = Sys.ActorOf(Query.TestActor.Props("a"));

        //    for (int i = 0; i < 150; ++i)
        //    {
        //        a.Tell("a green apple");
        //        ExpectMsg("a green apple-done");
        //    }

        //    var greenSrc = queries.CurrentEventsByTag("green", offset: NoOffset());
        //    var probe = greenSrc.RunWith(this.SinkProbe<EventEnvelope>(), Materializer);
        //    probe.Request(150);
        //    for (int i = 0; i < 150; ++i)
        //    {
        //        probe.ExpectNext<EventEnvelope>(p =>
        //            p.PersistenceId == "a" && p.SequenceNr == (i + 1) && p.Event.Equals("a green apple"));
        //    }

        //    probe.ExpectComplete();
        //    probe.ExpectNoMsg(TimeSpan.FromMilliseconds(500));
        //}
    }
}