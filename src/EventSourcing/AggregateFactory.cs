using System.Reflection;

namespace EventSourcing;

internal static class AggregateFactory 
{
    public static T Create<T>() where T : class 
    {
        var type = typeof(T);
        var ctor = type
            .GetConstructors(BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic)
            .Where(c => 
            {
                var p = c.GetParameters();
                return !(p.Length == 1 && p[0].ParameterType == type); // Copy constructor 
            })
            .OrderBy(c => c.GetParameters().Length)
            .FirstOrDefault() ?? throw new InvalidOperationException($"No suitable constructor found for type {type.Name}.");
             
        var parameters = ctor.GetParameters();
        object?[] args = [.. parameters.Select(p =>
            p.HasDefaultValue 
                ? p.DefaultValue 
                : (p.ParameterType.IsValueType 
                    ? Activator.CreateInstance(p.ParameterType) 
                    : null)
        )];
        
        return (T)ctor.Invoke(args);
    }
}