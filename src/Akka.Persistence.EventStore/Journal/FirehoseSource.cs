using Akka.Streams;
using Akka.Streams.Dsl;
using Akka.Streams.Stage;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Akka.Persistence.EventStore.Journal
{
    public static class FirehoseSourceExtensions
    {
        /// <summary>
        /// Repeatedly applies the specified function to every element until it succeeds, or downstream is cancelled
        /// </summary>
        /// <typeparam name="TOut"></typeparam>
        /// <param name="start">Where to start subscription from exclusive of the index itself (i.e. specifying 0 means start from second event). Null means start from beginning</param>
        /// <param name="source"></param>
        /// <param name="buffer">Buffer size</param>
        /// <param name="lowWaterMark">When buffer is below this point, a subscription will be made to top-up the buffer</param>
        /// <param name="highWaterMark">When buffer is above this point, the subscription will be dropped</param>
        /// <param name="autoRecover">Automatically recover when source indicated broken</param>
        /// <returns></returns>
        public static Source<TOut, NotUsed> AsBackpressureSource<TOut>(
            this FirehoseSource<TOut> source,
            long? start = null,
            long lowWaterMark = 250, long highWaterMark = 750, bool autoRecover = true)
        {
            return Source.FromGraph(new FirehoseSourceStage<TOut>(start, source.Subscribe, lowWaterMark, highWaterMark, autoRecover));
        }
    }

    public static class FirehoseSource
    {
        /// <summary>
        /// Define a firehose source with a function that starts it
        /// </summary>
        /// <typeparam name="TOut"></typeparam>
        /// <param name="subscribe">A function that creates a subscription to a firehose source from a starting point, with a callback 
        /// to notify new event along with its offset, returning an IDisposable that can unsubscribe</param>
        /// <returns></returns>
        public static FirehoseSource<TOut> Of<TOut>(Func<FirehoseSourceArgs<TOut>, Task<IDisposable>> subscribe) => new FirehoseSource<TOut>(subscribe);
    }

    public class FirehoseSource<TOut>
    {
        public FirehoseSource(Func<FirehoseSourceArgs<TOut>, Task<IDisposable>> subscribe) => Subscribe = subscribe;
        public Func<FirehoseSourceArgs<TOut>, Task<IDisposable>> Subscribe { get; private set; }
    }


    public class FirehoseSourceArgs<TOut>
    {
        /// <summary>
        /// Where to start the firehose from (exclusive offset, null for right at the beginning)
        /// </summary>
        public long? Start { get; private set; }

        /// <summary>
        /// Callback for the firehose to push items to
        /// </summary>
        public Action<(TOut item, long offset)> EventAppeared { get; private set; }

        /// <summary>
        /// Callback for the firehose to indicate it has been disconnected and the reason
        /// </summary>
        public Action<string> Broken { get; private set; }

        public static FirehoseSourceArgs<TOut> Create(long? start, Action<(TOut item, long offset)> eventAppeared, Action<string> broken)
            => new FirehoseSourceArgs<TOut>
            {
                EventAppeared = eventAppeared,
                Broken = broken,
                Start = start
            };
    }

    /// <summary>
    /// Converts a firehose source to a back-pressured Source
    /// </summary>
    /// <typeparam name="TOut"></typeparam>
    public class FirehoseSourceStage<TOut> : GraphStage<SourceShape<TOut>>
    {
        private sealed class Logic : OutGraphStageLogic
        {
            private FirehoseSourceStage<TOut> _stage;
            private IDisposable _subscription;

            private bool _subscribed, _full;
            private bool _downstreamWaiting = true;
            private int _epoch = 0;

            private Action<(long epoch, TOut item, long offset)> _eventAppeared;
            private Action<(long epoch, string reason)> _broken;
            private readonly Queue<(TOut, long)> _buffer = new Queue<(TOut, long)>();
            private long? _lastOffset;

            public Logic(FirehoseSourceStage<TOut> stage) : base(stage.Shape)
            {
                _stage = stage;
                _lastOffset = _stage._initialStart;
                SetHandler(stage.Out, this);
            }

            public override void PreStart()
            {
                base.PreStart();
                _eventAppeared = GetAsyncCallback<(long, TOut, long)>(OnEvent);
                _broken = GetAsyncCallback<(long, string)>(Broken);
            }

            public override void OnPull()
            {
                _downstreamWaiting = true;
                RunStep();
            }

            public override void PostStop()
            {
                Disconnect();
                base.PostStop();
            }

            private void RunStep()
            {
                TryPush();
                SubscribeOrUnsubscribe();
            }

            private void TryPush()
            {
                if (_buffer.Count > 0 && _downstreamWaiting)
                {
                    _downstreamWaiting = false;
                    var (nextUp, _) = _buffer.Dequeue();
                    Push(_stage.Out, nextUp);
                }
            }

            private void SubscribeOrUnsubscribe()
            {
                if ((_buffer.Count > _stage._highWaterMark) && _subscribed)
                {
                    _full = true;
                    Disconnect();
                }
                else if (_buffer.Count < _stage._lowWaterMark && !_subscribed)
                {
                    _full = false;
                    _subscribed = true;
                    var epoch = ++_epoch;
                    _ = Task.Run(async () =>
                    {
                        _subscription = await _stage._subscribe(FirehoseSourceArgs<TOut>.Create(_lastOffset, 
                            t => _eventAppeared((epoch, t.item, t.offset)), 
                            r => _broken((epoch, r))));
                    });
                }
            }

            private void Disconnect()
            {
                _subscription?.Dispose();
                _subscribed = false;
                _subscription = null;
            }

            private void OnEvent((long epoch, TOut item, long offset) value)
            {
                if (_epoch != value.epoch) return;
                if (!_full)
                {
                    _lastOffset = value.offset;
                    _buffer.Enqueue((value.item, value.offset));
                }
                RunStep();
            }

            private void Broken((long epoch, string reason) value)
            {
                var (epoch, reason) = value;
                if (_epoch != epoch) return;
                Disconnect();
                if (_stage._recover)
                {
                    SubscribeOrUnsubscribe();
                }
                else
                {
                    FailStage(new Exception(reason ?? "Firehose source indicated the subscription is broken"));
                }
            }
        }

        private readonly long? _initialStart;
        private readonly Func<FirehoseSourceArgs<TOut>, Task<IDisposable>> _subscribe;
        private readonly long _lowWaterMark;
        private readonly long _highWaterMark;
        private readonly bool _recover;

        public FirehoseSourceStage(
            long? start,
            Func<FirehoseSourceArgs<TOut>, Task<IDisposable>> subscribe,
            long lowWaterMark, long highWaterMark, bool recover)
        {
            Shape = new SourceShape<TOut>(Out);
            this._initialStart = start;
            this._subscribe = subscribe;
            this._lowWaterMark = lowWaterMark;
            this._highWaterMark = highWaterMark;
            this._recover = recover;
        }

        public Outlet<TOut> Out { get; } = new Outlet<TOut>("Source.out");

        public override SourceShape<TOut> Shape { get; }

        protected override GraphStageLogic CreateLogic(Attributes inheritedAttributes) => new Logic(this);
    }
}
