using Akka.Persistence.EventStore.Journal;
using Akka.Streams;
using Akka.Streams.Dsl;
using Akka.Streams.TestKit;
using Akka.Util;
using FluentAssertions;
using System;
using System.Linq;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace Akka.Persistence.EventStore.Tests
{
    public class FirehoseBufferSpec : Akka.TestKit.Xunit2.TestKit
    {
        private static readonly Random rand = new Random();

        public FirehoseBufferSpec(ITestOutputHelper output) : base(output: output) { }

        [Theory]
        [InlineData(0, 0)] // Fast source
        [InlineData(0, 0.1)] // Fast source with random failure
        [InlineData(10, 0)] // Slow source
        [InlineData(10, 0.1)] // Slow source with random failure
        public void FirehoseSource_should_output_all_events(int intervalMs, double randomFailureRatio)
        {
            var count = 1_000;
            var mat = Sys.Materializer();
            var probe = FirehoseSource.Of<int>(args => GetFirehose(args, mat, intervalMs, randomFailureRatio))
                .AsBackpressureSource(null, 25, 75, autoRecover: true)
                .Take(count)
                .RunWith(this.SinkProbe<int>(), mat);

            for (int i = 0; i < count; ++i)
            {
                probe.Request(1);
                probe.ExpectNext(TimeSpan.FromSeconds(30)).Should().Be(i);
            }
            probe.ExpectComplete();
        }




        [Fact]
        public void FirehoseSource_should_cancel_when_downstream_cancels()
        {
            var count = 1_000;
            var mat = Sys.Materializer();
            var (killSwitch, probe) = FirehoseSource.Of<int>(args => GetFirehose(args, mat, intervalInMs: 5, failureRatio: 0))
                .AsBackpressureSource(null, 25, 75, autoRecover: true)
                .Take(count)
                .ViaMaterialized(KillSwitches.Single<int>(), Keep.Right)
                .ToMaterialized(this.SinkProbe<int>(), Keep.Both)
                .Run(mat);

            var demand = 51;
            probe.Request(demand);
            for (int i = 0; i < demand; ++i)
            {
                probe.ExpectNext().Should().Be(i);
            }

            killSwitch.Shutdown();
            probe.ExpectComplete();
        }

        [Fact]
        public void FirehoseSource_should_fail_if_not_autoRecover()
        {
            var count = 1_000;
            var mat = Sys.Materializer();
            var (killSwitch, probe) = FirehoseSource.Of<int>(args => GetFirehose(args, mat, intervalInMs: 5, failureRatio: 1))
                .AsBackpressureSource(null, 25, 75, autoRecover: false)
                .Take(count)
                .ViaMaterialized(KillSwitches.Single<int>(), Keep.Right)
                .ToMaterialized(this.SinkProbe<int>(), Keep.Both)
                .Run(mat);

            probe.Request(1);
            probe.ExpectError();
        }










        public Task<IDisposable> GetFirehose(FirehoseSourceArgs<int> args, ActorMaterializer mat, double intervalInMs, double failureRatio)
        {
            var start = args.Start == null ? 0 : (int)args.Start.Value + 1;

            var ks = KillSwitches.Shared("FirehoseTest");
            _ = Source.From(Enumerable.Range(start, int.MaxValue - start))
                .Zip(intervalInMs == 0
                    ? Source.From(Enumerable.Repeat("", int.MaxValue))
                    : Source.Tick(TimeSpan.Zero, TimeSpan.FromMilliseconds(intervalInMs), "").MapMaterializedValue(v => NotUsed.Instance))
                .Select(t => t.Item1)
                .Select(i =>
                {
                    if (failureRatio == 0 || rand.NextDouble() > failureRatio)
                        return i;
                    else
                        throw new Exception("Random failure");
                })
                .Via(ks.Flow<int>())
                .Recover(ex =>
                {
                    args.Broken(ex.Message);
                    return Option<int>.None;
                })
                .RunWith(Sink.ForEach<int>(i =>
                {
                    args.EventAppeared((i, i));
                }), mat);

            return Task.FromResult<IDisposable>(DisposableKillSwitch.Of(ks));
        }

        private class DisposableKillSwitch : IDisposable
        {
            public IKillSwitch KillSwitch { get; private set; }
            public static DisposableKillSwitch Of(IKillSwitch inner) => new DisposableKillSwitch { KillSwitch = inner };
            public void Dispose() => KillSwitch.Shutdown();
        }
    }
}
