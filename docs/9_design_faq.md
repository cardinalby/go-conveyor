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

## Q4. FanOut.MoveTo

Why does `FanOut.MoveTo` take no tasks? Why the extra `Schedule` call?

---

`MoveTo` moves; `Schedule` adds work. Splitting the two is what makes a growing body possible:
- the ItemProcessor may enter, look at the data, and only then decide which tasks to build (a conditional body);
- it may schedule in **rounds** (`Schedule`, `Wait`, `Schedule` again) and use the result of one round to plan the
  next;
- a **task** may `Schedule` follow-ups into the same body with its own context, so a tree of work can grow while the
  item stays inside the fan-out.

None of that fits one call that both enters and hands over a fixed set of tasks. The cost is the **door**: the item
behind cannot enter the fan-out until the item ahead has scheduled once (or left, or detached), so branch work stays
in item order. Keep the code between `MoveTo` and the first `Schedule` short.

## Q5. Waiting inside a task

Why can a task not wait for the work it spawned?

---

A running task holds a branch slot. If it could wait for a follow-up, and the follow-up needed a slot of the same
pool, the pool could be full of tasks all waiting for follow-ups that never start: a hold-and-wait cycle. The
deadlock-freedom argument of the library rests on the opposite: a running task waits for nothing, and a queued task
holds nothing. So `Wait` (and every move) with a task's context panics.

What a task may do is `Schedule` — a non-blocking enqueue. If a step needs all the siblings done, make that step a
task of its own and let the last sibling schedule it (see [Join as a continuation](4_fan-out.md#join-as-a-continuation)).
The ItemProcessor, which holds no branch slot, is the one that waits: with `Wait`, or by leaving the fan-out.

## Q6. Stripped contexts

Why does a context with cancellation stripped (`context.WithoutCancel`) not bypass cancellation?

---

Cancellation is judged by the **item**, not by the context you pass. Every node method also reads the cancellation
cause of the item's own context, so once a task has failed or the conveyor is shutting down, `MoveTo`, `TryMoveTo`,
`Schedule` and `Wait` return the cause, and `Retain` declines to run its callback, whatever the caller derived from
the item's context.

The reason is ordering. If item 5 fails while writing and item 6 hides its cancellation to reach `commit`, item 6
would commit cumulative offsets past item 5's messages. A pipeline is only correct if a canceled item cannot enter a
node. Code that must run after cancellation can still run in plain Go once the node method has returned; it just
cannot run inside a node. A derived context with a shorter deadline still works, because it inherits the item's
cancellation and adds its own.

---

| Prev                             |
|----------------------------------|
| [⬅ Benchmarks](8_benchmarks.md) |