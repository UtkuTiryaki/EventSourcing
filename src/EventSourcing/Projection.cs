namespace EventSourcing;

public abstract record Projection(EventStream EventStream) 
{
    public abstract IReadmodel? Project(Guid aggregateRootId);
}

public interface IReadmodel 
{
    public Guid Id { get; }
}
