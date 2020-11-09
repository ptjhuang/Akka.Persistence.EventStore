using Akka.Streams;
using Akka.Streams.Dsl;
using Akka.Streams.Implementation.Fusing;
using Akka.Streams.Stage;
using Akka.Streams.Supervision;
using System;

namespace Akka.Persistence.EventStore.Journal
{
    public static class FlowOperations
    {
        public static IFlow<T, TMat> TakeUntil<T, TMat>(this IFlow<T, TMat> flow, Predicate<T> predicate, bool inclusive)
        {
            return flow.Via(new TakeUntil<T>(predicate, inclusive));
        }
    }

    public static class SourceOperations
    {
        public static Source<TOut, TMat> TakeUntil<TOut, TMat>(this Source<TOut, TMat> flow, Predicate<TOut> predicate, bool inclusive = false)
        {
            return (Source<TOut, TMat>)FlowOperations.TakeUntil(flow, predicate, inclusive);
        }
    }

    /// <summary>
    /// Take until
    /// </summary>
    /// <typeparam name="T">TBD</typeparam>
    public sealed class TakeUntil<T> : SimpleLinearGraphStage<T>
    {
        #region Logic

        private sealed class Logic : InAndOutGraphStageLogic
        {
            private readonly TakeUntil<T> _stage;
            private readonly Decider _decider;

            public Logic(TakeUntil<T> stage, Attributes inheritedAttributes) : base(stage.Shape)
            {
                _stage = stage;
                var attr = inheritedAttributes.GetAttribute<ActorAttributes.SupervisionStrategy>(null);
                _decider = attr != null ? attr.Decider : Deciders.StoppingDecider;

                SetHandler(stage.Outlet, this);
                SetHandler(stage.Inlet, this);
            }

            public override void OnPush()
            {
                try
                {
                    var element = Grab(_stage.Inlet);
                    if (!_stage._predicate(element))
                        Push(_stage.Outlet, element);
                    else
                    {
                        if (_stage._inclusive)
                            Push(_stage.Outlet, element);

                        CompleteStage();
                    }
                }
                catch (Exception ex)
                {
                    if (_decider(ex) == Directive.Stop)
                        FailStage(ex);
                    else
                        Pull(_stage.Inlet);
                }
            }

            public override void OnPull() => Pull(_stage.Inlet);

            public override string ToString() => "TakeUntilLogic";
        }

        #endregion

        private readonly Predicate<T> _predicate;
        private readonly bool _inclusive;

        /// <summary>
        /// TBD
        /// </summary>
        /// <param name="predicate">TBD</param>
        /// <param name="inclusive">TBD</param>
        public TakeUntil(Predicate<T> predicate, bool inclusive)
        {
            _inclusive = inclusive;
            _predicate = predicate;
        }

        /// <summary>
        /// TBD
        /// </summary>
        protected override Attributes InitialAttributes { get; } = Attributes.CreateName("takeUntil");

        /// <summary>
        /// TBD
        /// </summary>
        /// <param name="inheritedAttributes">TBD</param>
        /// <returns>TBD</returns>
        protected override GraphStageLogic CreateLogic(Attributes inheritedAttributes)
            => new Logic(this, inheritedAttributes);

        /// <summary>
        /// Returns a <see cref="string" /> that represents this instance.
        /// </summary>
        /// <returns>
        /// A <see cref="string" /> that represents this instance.
        /// </returns>
        public override string ToString() => "TakeUntil";
    }
}
