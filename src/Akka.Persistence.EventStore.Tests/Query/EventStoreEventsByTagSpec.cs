using Akka.Actor;
using Akka.Configuration;
using Akka.Persistence.EventStore.Query;
using Akka.Persistence.Query;
using Akka.Persistence.TCK.Query;
using Akka.Streams;
using Akka.Streams.TestKit;
using System;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace Akka.Persistence.EventStore.Tests.Query
{
    public class EventStoreEventsByTagSpec : EventsByTagSpec, IClassFixture<DatabaseFixture>, IAsyncLifetime
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

        public EventStoreEventsByTagSpec(DatabaseFixture databaseFixture, ITestOutputHelper output) : 
                base(Config(databaseFixture), nameof(EventStoreEventsByTagSpec), output)
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
        public virtual void ReadJournal_live_query_should_dispose()
        {
            var rnd = new Random();
            var queries = ReadJournal as IEventsByTagQuery;
            var a = Sys.ActorOf(Query.TestActor.Props("a"));
            var subscriptions = 150; // gRPC usually has a 100 connection limit
            var count = 1;

            for (var s = 0; s < subscriptions; ++s)
            {
                var probe = queries.EventsByTag("green", offset: Offset.NoOffset())
                    .RunWith(this.SinkProbe<EventEnvelope>(), Sys.Materializer());

                for (int i = 0; i < count; ++i)
                {
                    a.Tell("a green apple");
                    ExpectMsg("a green apple-done");
                }

                probe.Request(count);
                for (int i = 0; i < count; ++i)
                {
                    probe.ExpectNext<EventEnvelope>(e => ((Sequence)e.Offset).Value == i);
                }

                probe.Cancel();
            }
        }
    }
}