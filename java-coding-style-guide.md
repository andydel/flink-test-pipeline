# Java Coding Style Guide

> **Purpose:** This guide defines general Java coding conventions for AI-assisted and human-written code. Prefer clear, modern, maintainable Java over clever or overly abstract solutions.

## 1. General Principles

- Write code for readability and maintainability first.
- Prefer simple, explicit designs over unnecessary abstraction.
- Follow SOLID principles where they improve the design, but do not over-engineer.
- Keep classes and methods focused on a single responsibility.
- Prefer composition over inheritance.
- Make invalid states difficult to represent.
- Use immutable data where practical.
- Avoid premature optimisation.
- Do not introduce frameworks, dependencies, patterns, or abstraction layers without a clear benefit.
- When modifying existing code, follow the established project conventions unless they conflict with an explicit requirement in this guide.

## 2. Java Version and Language Features

- Prefer the latest Java language features supported by the project.
- Use modern Java constructs where they improve clarity.
- Prefer:
  - records for simple immutable data carriers where Lombok behaviour is not required;
  - switch expressions over verbose switch statements;
  - pattern matching where supported;
  - `var` for local variables when the type is obvious from the right-hand side.
- Do not use `var` where it obscures the meaning or type of a value.

## 3. Naming

Use standard Java naming conventions:

- Classes, records, enums and interfaces: `PascalCase`
- Methods and variables: `camelCase`
- Constants: `UPPER_SNAKE_CASE`
- Packages: lowercase, e.g. `com.example.orders`
- Boolean values should normally read naturally as predicates:
  - `isActive`
  - `hasPermission`
  - `canRetry`
- Prefer descriptive names over abbreviations.
- Avoid meaningless names such as `data`, `obj`, `tmp`, `thing`, or `manager` unless the meaning is genuinely obvious.
- Methods should normally describe an action, e.g. `calculateTotal()` or `findCustomer()`.

## 4. Formatting

- Use 4 spaces for indentation. Do not use tabs.
- Use braces for control-flow statements, including single-line bodies.
- Keep lines reasonably short; aim for approximately 120 characters or fewer.
- Put one public top-level type in each source file.
- Separate logical sections of code with whitespace, but avoid excessive blank lines.
- Use automated formatting where the project provides it.

## 5. Lombok

**Lombok is preferred** where it removes repetitive boilerplate without hiding important behaviour.

Prefer Lombok annotations such as:

```java
@Getter
@Setter
@Builder
@RequiredArgsConstructor
@AllArgsConstructor
@NoArgsConstructor
@Value
@Slf4j
```

Guidelines:

- Prefer `@RequiredArgsConstructor` for constructor dependency injection.
- Prefer `@Slf4j` for logging.
- Prefer `@Builder` where objects have several optional or named construction parameters.
- Prefer `@Value` for immutable value objects when a Java `record` is not more appropriate.
- Avoid `@Data` by default. It generates setters, `equals`, `hashCode`, and `toString` together and can expose more behaviour than intended.
- Prefer targeted annotations such as `@Getter`, `@Setter`, and `@RequiredArgsConstructor`.
- Be cautious with Lombok-generated `equals`, `hashCode`, and `toString` on JPA entities or objects containing lazy-loaded relationships.
- Do not manually write boilerplate that Lombok can safely and clearly generate.
- Do not use Lombok where generated behaviour would make the class contract unclear.

Example:

```java
@Service
@RequiredArgsConstructor
@Slf4j
public class OrderService {

    private final OrderRepository orderRepository;
    private final PaymentService paymentService;
}
```

## 6. Lambda Expressions and Functional Style

**Prefer lambda expressions and method references** for concise behaviour, collection transformations, callbacks, and functional interfaces.

Prefer:

```java
orders.stream()
        .filter(Order::isActive)
        .map(Order::getCustomerId)
        .distinct()
        .toList();
```

over unnecessarily imperative code:

```java
List<Long> customerIds = new ArrayList<>();

for (Order order : orders) {
    if (order.isActive() && !customerIds.contains(order.getCustomerId())) {
        customerIds.add(order.getCustomerId());
    }
}
```

Guidelines:

- Prefer lambdas for implementations of functional interfaces.
- Prefer method references when they are clearer than the equivalent lambda.

```java
customers.forEach(this::processCustomer);
```

rather than:

```java
customers.forEach(customer -> processCustomer(customer));
```

- Keep lambda bodies short.
- Extract complex lambda logic into a named method.
- Do not force a stream/lambda solution when a conventional loop is substantially clearer.
- Avoid side effects inside stream operations where practical.
- Prefer transformations such as `map`, `filter`, `flatMap`, `reduce`, and collectors over mutable state.
- Prefer `toList()` on modern Java versions when an unmodifiable result is acceptable.

## 7. Collections

- Program to interfaces:

```java
List<Customer> customers = new ArrayList<>();
Map<String, Customer> customersById = new HashMap<>();
```

- Prefer immutable collections when mutation is unnecessary.
- Use `List.of()`, `Set.of()`, and `Map.of()` for small immutable collections.
- Avoid returning `null` collections. Return an empty collection instead.
- Choose the collection type based on semantics, not habit.
- Use streams where they make transformations easier to understand.

## 8. Null Handling and Optional

- Avoid returning `null` where a better representation exists.
- Use `Optional<T>` primarily for return values where absence is expected.
- Do not generally use `Optional` for:
  - fields;
  - method parameters;
  - DTO properties.
- Do not call `Optional.get()` without first establishing that a value exists.
- Prefer functional Optional operations:

```java
return customerRepository.findById(customerId)
        .map(this::toResponse)
        .orElseThrow(() -> new CustomerNotFoundException(customerId));
```

- Validate required values at system boundaries.

## 9. Methods

- Keep methods small and focused.
- A method should normally perform one conceptual operation.
- Prefer early returns to deeply nested conditional logic.
- Avoid large parameter lists. Consider a parameter object or domain object when parameters naturally belong together.
- Avoid boolean parameters when they make calls ambiguous:

```java
processOrder(order, true);
```

Prefer a meaningful type, enum, or separate method.
- Do not create trivial wrapper methods that add no semantic value.

## 10. Classes and Interfaces

- Keep classes cohesive.
- Prefer dependency injection over constructing dependencies internally.
- Prefer constructor injection.
- Declare dependencies `final`.
- Avoid unnecessary inheritance hierarchies.
- Use interfaces where there is a genuine abstraction boundary or multiple implementations are plausible.
- Do not create an interface solely because a class exists.
- Keep public APIs as small as possible.

## 11. Immutability

Prefer immutable objects where practical.

- Use `final` fields.
- Avoid setters unless mutation is part of the object's intended behaviour.
- Prefer records or Lombok `@Value` for value objects.
- Do not expose mutable internal collections directly.
- Prefer returning immutable views or copies where appropriate.

## 12. Exceptions

- Use exceptions for exceptional conditions, not normal control flow.
- Prefer domain-specific exceptions where they improve diagnostics.
- Do not catch `Exception` or `Throwable` unless there is a specific boundary-level reason.
- Never silently swallow exceptions.
- Preserve the original cause when wrapping exceptions:

```java
throw new OrderProcessingException("Unable to process order " + orderId, exception);
```

- Error messages should contain useful context without exposing sensitive information.

## 13. Logging

Prefer Lombok `@Slf4j`.

Use parameterised logging:

```java
log.info("Processing order {} for customer {}", orderId, customerId);
```

Do not use string concatenation:

```java
log.info("Processing order " + orderId);
```

Guidelines:

- `ERROR`: operation failed and requires attention.
- `WARN`: unexpected situation that can be handled.
- `INFO`: meaningful application or business lifecycle event.
- `DEBUG`: diagnostic information useful during development/support.
- `TRACE`: highly detailed diagnostic information.
- Do not log passwords, credentials, access tokens, secrets, or unnecessary personal data.
- Avoid excessive logging inside loops or high-volume processing paths.

## 14. Comments and Documentation

- Prefer self-documenting code over comments.
- Comments should explain **why**, not restate **what** the code does.
- Remove stale or commented-out code.
- Use Javadoc for public APIs when their behaviour, constraints, or usage are not obvious.
- Document surprising design decisions and important business rules.

Bad:

```java
// Increment count
count++;
```

Better:

```java
// Retry count is persisted before execution so a process crash cannot reset the retry limit.
retryCount++;
```

## 15. Dependency Injection

For Spring applications, prefer constructor injection using Lombok:

```java
@Component
@RequiredArgsConstructor
public class CustomerProcessor {

    private final CustomerRepository customerRepository;
    private final NotificationService notificationService;
}
```

Avoid field injection:

```java
@Autowired
private CustomerRepository customerRepository;
```

Dependencies should be explicit and testable.

## 16. DTOs and Domain Models

- Keep API DTOs separate from persistence entities where the models have different responsibilities.
- Prefer records for simple request/response DTOs when supported:

```java
public record CustomerResponse(
        UUID id,
        String name,
        String email
) {}
```

- Do not expose persistence entities directly through external APIs.
- Keep business logic in domain/service components rather than controllers or DTOs.

## 17. Streams

Use streams for declarative collection processing.

Good:

```java
var activeCustomers = customers.stream()
        .filter(Customer::isActive)
        .sorted(comparing(Customer::getName))
        .toList();
```

Avoid excessively long pipelines. If a stream becomes difficult to read:

- introduce named helper methods;
- break the operation into meaningful stages; or
- use conventional control flow.

Avoid using `peek()` for business logic or mutation.

## 18. Concurrency

- Prefer high-level concurrency utilities over manual thread management.
- Use executors, `CompletableFuture`, virtual threads, or framework-managed concurrency as appropriate to the project's Java version.
- Avoid shared mutable state.
- Make thread-safety assumptions explicit.
- Do not add asynchronous processing unless it provides a real benefit.
- Handle timeouts, cancellation, and exceptions in asynchronous workflows.

## 19. Testing

- Write unit tests for meaningful business logic.
- Use clear Arrange / Act / Assert structure.
- Test behaviour rather than implementation details.
- Prefer descriptive test names.

Example:

```java
@Test
void shouldRejectOrderWhenCustomerHasInsufficientCredit() {
    // Arrange
    ...

    // Act
    ...

    // Assert
    ...
}
```

- Prefer parameterised tests for repeated scenarios.
- Mock external dependencies, not simple value objects or the class under test.
- Avoid excessive mocking.
- Include tests for boundary conditions and failure paths.
- A bug fix should normally include a regression test.

## 20. Security

- Never hard-code credentials or secrets.
- Validate and sanitise external input as appropriate.
- Use parameterised SQL or ORM query parameters.
- Apply least-privilege principles.
- Avoid exposing internal exception details to API clients.
- Treat deserialised input as untrusted.
- Keep dependencies current and avoid unnecessary dependencies.

## 21. Database and Persistence Code

- Keep transaction boundaries explicit.
- Avoid N+1 query patterns.
- Do not load significantly more data than required.
- Use database constraints as well as application validation for critical integrity rules.
- Keep persistence concerns out of controllers.
- Be cautious with lazy-loaded entities outside transaction boundaries.
- Prefer clear repository/query methods over hidden or surprising persistence behaviour.

## 22. API Code

- Keep controllers thin.
- Controllers should normally:
  1. validate/accept the request;
  2. delegate to application/service logic;
  3. map the result to the API response.
- Use appropriate HTTP status codes.
- Use consistent error-response structures.
- Do not expose implementation details in public contracts.
- Version APIs only where necessary and according to project conventions.

## 23. Constants and Magic Values

Avoid unexplained literals:

```java
if (retryCount > 5) {
    ...
}
```

Prefer:

```java
private static final int MAX_RETRY_COUNT = 5;
```

For domain concepts, prefer enums or value types rather than arbitrary strings or integers.

## 24. AI Code Generation Rules

When generating or modifying Java code:

- Inspect and follow the existing codebase conventions first.
- Prefer Lombok for safe boilerplate reduction.
- Prefer lambda expressions, method references, streams, and modern Java syntax where they improve readability.
- Do not rewrite clear imperative code into functional code solely for stylistic consistency.
- Do not add unnecessary abstractions, factories, interfaces, builders, or design patterns.
- Reuse existing project utilities and dependencies before introducing new ones.
- Do not add a new dependency without a clear reason.
- Preserve backward compatibility unless explicitly instructed otherwise.
- Keep changes narrowly scoped to the requested task.
- Do not silently alter unrelated code.
- Add or update tests when behaviour changes.
- Ensure generated code compiles conceptually: imports, types, nullability, checked exceptions, generics, and method signatures must be consistent.
- Prefer explicit, understandable code over clever one-liners.
- Flag assumptions when requirements cannot be inferred safely.

## 25. Preferred Example Style

```java
@Service
@RequiredArgsConstructor
@Slf4j
public class CustomerService {

    private final CustomerRepository customerRepository;

    public List<CustomerResponse> findActiveCustomers() {
        log.debug("Finding active customers");

        return customerRepository.findAll().stream()
                .filter(Customer::isActive)
                .map(this::toResponse)
                .toList();
    }

    private CustomerResponse toResponse(Customer customer) {
        return new CustomerResponse(
                customer.getId(),
                customer.getName(),
                customer.getEmail()
        );
    }
}
```

## 26. Priority Order

When rules appear to conflict, use this priority:

1. Correctness and security.
2. Explicit project requirements.
3. Existing codebase conventions and architectural decisions.
4. Readability and maintainability.
5. This style guide.
6. Conciseness.

The goal is **consistent, idiomatic, modern Java that is easy for another developer to understand and change**.
