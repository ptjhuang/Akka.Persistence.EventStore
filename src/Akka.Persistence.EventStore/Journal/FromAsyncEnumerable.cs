using Akka.Streams;
using Akka.Streams.Dsl;
using Akka.Streams.Stage;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace Akka.Persistence.EventStore.Journal
{
    public static class AsyncEnumerableExtensions
    {
        /// <summary>
        /// Convert an async enumerable to Source
        /// </summary>
        /// <typeparam name="TOut"></typeparam>
        /// <param name="enumerable"></param>
        /// <returns></returns>
        public static Source<TOut, NotUsed> AsStreamSource<TOut>(this IAsyncEnumerable<TOut> enumerable)
        {
            return Source.FromGraph(new AsyncEnumerableSourceStage<TOut>(enumerable));
        }
    }


    /// <summary>
    /// Converts a firehose source to a back-pressured Source
    /// </summary>
    /// <typeparam name="TOut"></typeparam>
    public class AsyncEnumerableSourceStage<TOut> : GraphStage<SourceShape<TOut>>
    {
        private sealed class Logic : OutGraphStageLogic
        {
            private AsyncEnumerableSourceStage<TOut> _stage;
            private Action<TOut> _onNext;
            private Action _onComplete;
            private IAsyncEnumerable<TOut> _enumerable;
            private IAsyncEnumerator<TOut> _enumerator;

            public Logic(AsyncEnumerableSourceStage<TOut> stage) : base(stage.Shape)
            {
                _stage = stage;
                _enumerable = stage._enumerable;
                SetHandler(stage.Out, this);
            }

            public override void PreStart()
            {
                base.PreStart();
                _onNext = GetAsyncCallback<TOut>(OnNext);
                _onComplete = GetAsyncCallback(CompleteStage);

                _enumerator = _enumerable.GetAsyncEnumerator();
            }

            public override void OnPull() => Task.Run(async () =>
            {
                if (await _enumerator.MoveNextAsync())
                    _onNext(_enumerator.Current);
                else
                    _onComplete();
            });

            private void OnNext(TOut item)
            {
                Push(_stage.Out, item);
            }

            public override void OnDownstreamFinish()
            {
                _enumerator.DisposeAsync();
                base.OnDownstreamFinish();
            }
        }

        private readonly IAsyncEnumerable<TOut> _enumerable;

        public AsyncEnumerableSourceStage(IAsyncEnumerable<TOut> enumerable)
        {
            Shape = new SourceShape<TOut>(Out);
            _enumerable = enumerable;
        }

        public Outlet<TOut> Out { get; } = new Outlet<TOut>("Source.out");

        public override SourceShape<TOut> Shape { get; }

        protected override GraphStageLogic CreateLogic(Attributes inheritedAttributes) => new Logic(this);
    }
}
