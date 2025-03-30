namespace EventSourcing;

public record EventStream(Guid AggregateId, IReadOnlyList<DomainEvent> Events) 
{
    public EventStream Append(IEnumerable<DomainEvent> newEvents)
        => this with { Events = [.. Events, .. newEvents] };
        
    public TAggregate Replay<TAggregate>(TAggregate initial, Func<TAggregate, DomainEvent, TAggregate> apply) => 
        Events.Aggregate(initial, apply);
        
    public TAggregate Replay<TAggregate>() where TAggregate : AggregateRoot
        => Replay(AggregateFactory.Create<TAggregate>(), (agg, e) => (TAggregate)agg.Apply(e));
        
    public static EventStream Create(Guid aggregateId) => new(aggregateId, []);
}