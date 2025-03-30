using MediatR;

namespace EventSourcing;

public interface IEntity 
{
    Guid Id { get; }   
}


public interface IAggregateRoot : IEntity
{
    
}

public abstract record DomainEvent : INotification;

public abstract record AggregateRoot : IAggregateRoot 
{
    public Guid Id { get; init; }
    public IReadOnlyList<DomainEvent> UncommittedEvents { get; init; } = [];
    
    public abstract AggregateRoot Apply(DomainEvent domainEvent);
    
    public T AddDomainEvent<T>(DomainEvent domainEvent) where T : AggregateRoot 
    {
        var updated = (T)this.Apply(domainEvent);
        var updatedEvents = updated.UncommittedEvents.Concat([domainEvent]).ToList();
        return updated with { UncommittedEvents = updatedEvents } as T;
    }
    
    public T LoadFromHistory<T>(IEnumerable<DomainEvent> history) where T : AggregateRoot 
    {
        T aggregate = (T)this;
        foreach (var domainEvent in history) 
        {
            aggregate = (T)aggregate.Apply(domainEvent);
        }
        
        return aggregate with { UncommittedEvents = [] } as T;
    }
}