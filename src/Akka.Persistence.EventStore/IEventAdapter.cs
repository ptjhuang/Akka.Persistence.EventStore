using Akka.Actor;
using EventStore.Client;
using System;

namespace Akka.Persistence.EventStore
{
    public interface IEventAdapter
    {
        EventData Adapt(IPersistentRepresentation persistentMessage);
        IPersistentRepresentation Adapt(ResolvedEvent @event);
    }
}
