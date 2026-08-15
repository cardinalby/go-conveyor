The doc answers questions about the chosen API design.

## Q1. MoveTo
Why `stage.MoveTo` semantics instead of:
- more classical `stage.Enter`/`stage.Exit` pair
- or `stage.Do(func () {...})` callback-style semantics?

---

Different options were considered.

Enter/Exit pair:
- more verbose and more difficult to use correctly (see below)
- to provide back-pressure correctly `prev.Exit` should be called only after `next.Enter`
- no work should normally be done between `prev.Exit` and `next.Enter` (rare "Retain" case)
- `defer stage.Exit()` doesn't work well (unlike unlocking a mutex) since you need to exit the previous stage
   after entering the next one, not after the entire `ItemProcessor` function returns
- doesn't work well with FanOut stage
- asymmetry:
  - initial "read" stage doesn't require Enter but requires Exit.
  - Exit for the final "commit" stage looks excessive. What if a user forgets to call it?

Do() callback-style:
- A user can add some code between `Do` calls - it will be executed out of any stage (breaks back-pressure).
- the main pro of "per-item" execution (opposite to chans) is that you can store the state of item processing 
  in local variables inside ItemProcessor and use them at any stage. Variables defined in one `Do` callback will 
  not be visible in another, which forces you to pre-define variables at the top of the function.
- One of the goals of the lib is to support easy migration from old 1-goroutine sequential code. 
  In case of `MoveTo`, you don't need to extract stages to functions (and extract variables); you can 
  just put `MoveTo` calls at the boundaries.
- the initial "read" stage is implicit and can't support `Do` semantics anyway.
- Retain functionality will look less natural.
- Questions arise: how to react if `Do` is called inside another `Do`?

## Q2. SetLimit

Why does SetLimit have a different meaning for Lanes/Pools and for Stages? Can it be used to process multiple messages
from a batch (represented by an item) in parallel?

---

The reason is that there are different units of work:
- **Stage**: a single item (created at the "read" stage), e.g. a batch of messages
- **Lane**/**Pool**: a single task (like a message inside the batch)

## Q3. Backpressure

How can I implement a smarter back-pressure strategy?

---

The basic back-pressure strategy (same as for channels) is:
- an item can't enter a stage if the stage is full (has reached its limit) and its queue is full (if any)
- it means it can't leave the previous stage and the previous stage is blocked for new items as well
- it propagates back to the "read" stage and no new workers are started until the "read" stage is available again

There are smarter strategies possible that slow down the "read" stage before the next stage is full.
The lib doesn't provide such strategies out of the box, but you can build your own back-pressure manager by 
using [observability metrics](./7_observability.md) and dynamically adjusted limits and queue sizes for stages.

---

| Prev                             |
|----------------------------------|
| [⬅ Benchmarks](8_benchmarks.md) |