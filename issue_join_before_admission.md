# Issue: a listed wave's error is not acknowledged when `MoveTo` fails before the join

Status: decided and implemented on 2026-09-10 (phase 12, see `design_fanout_schedule.md` §11.14). Found on 2026-09-10 while stabilizing `TestJoinOfFailedWaveAcknowledgesIt`
(phase 11 of `design_fanout_schedule.md`). Related spec sections: §6.1 step 7, §6.4, §6.7.

## Problem

`MoveTo(ctx, joins...)` does two things in this order:

1. wait for admission to the target node (a free slot and the ordering gate);
2. once inside, join the listed waves and return the first error.

A task failure poisons the item: the item's context is canceled with the task's error as the cause. Every blocking
wait of the item wakes and returns that cause.

- **Case A: the task fails during step 2.** The join wakes, sees the wave finished with an error, marks it observed
  (`acked`) and returns it. If the processor handles the error and returns nil, the item completes cleanly.
- **Case B: the task fails during step 1.** The admission wait wakes and `MoveTo` returns the cause at once. The join
  never ran, so the wave stays unobserved. The processor receives the same error value (the poison cause is the
  wave's error), but when it returns nil, completion finds an unacknowledged wave error and fails the item with it
  anyway.

Example, `commit` has limit 1 and another item holds it for a long time:

```go
w := fo.Detach(ctx)          // task still running
err := commit.MoveTo(ctx, w) // blocks waiting for a slot in commit
// the task fails with "boom" while we wait for the slot
// err == boom, but w was never joined
if errors.Is(err, boom) {
    log.Println("handled")
    return nil               // the item still fails with boom at completion
}
```

With a free slot the item would already be blocked in the join when the task fails, and the same code would end
with a clean item. The outcome depends on timing the processor cannot see.

Why it is accepted today: the library promises that an error is never lost. In case B nobody read the wave, so the
runtime cannot know the processor understood that the returned cause came from `w`. Failing the item is the safe
default. The reliable way to handle a detached wave's error is `<-w.Finished()` then `w.Err()`, or returning the
join error.

The same gap exists for `TryMoveTo`: a canceled item gets `(false, cause)` from the preamble and its listed waves
are not looked at. Its contract already says joins are awaited only if the item entered.

## Options

### 1. Keep as is

Joins are observed only if the join step ran. Simple rule, documented in §6.7. The trap above stays.

### 2. Join first, then admit

`MoveTo(ctx, w)` waits for `w` while the item still stands in the previous node, and asks for a slot in the target
only when `w` is done and clean. The library already does this for the item's own fan-out body: the leave waits for
the body to go idle and seals it before the admission wait (§6.4). Listed waves would follow the same order.

Pros:

- The two kinds of join become consistent.
- Case B disappears: the join runs first, and a poison from the listed wave is observed by it.
- A failed item never enters the target.

Cons:

- The item holds the previous node's slot while waiting for the wave, instead of the target's slot. Throughput is
  similar, but backpressure moves one node back: a slow wave blocks the item behind at `mid`, not at `commit`.
- `TryMoveTo` cannot follow. It must return at once if it cannot enter and awaits joins only after entering. Joining
  first would make it block, so `MoveTo` and `TryMoveTo` would order the two steps differently.
- §6.1 step 7 and the docs describe admission then join; both need rewording, and tests that assert where the item
  stands during a join change.

### 3. On a cancellation during admission, look at the listed waves (recommended)

If the admission wait returns a cause, check the joins before returning. Any listed wave that is finished with an
error is acknowledged and its error returned, node-qualified, as a join error would be. Otherwise the cause is
returned as today.

This is the rule `Wait`, the leave (`joinPending`) and `join` already apply after a cancellation wake-up: a finished
wave with an error is the truer message. About five lines in `enterUnit`, no change to where the item waits, no
change to the `TryMoveTo` contract (it may get the same check in its preamble, or stay as is).

Cost: a wave can be acknowledged by a call that never entered the target. Acceptable, because the caller receives
the wave's own error, so "an error is never lost" holds.

Test to add: the task fails while the item is still waiting for a slot in `commit` (another item holds it), then
the processor returns nil; the run must end clean. This is the case `TestJoinOfFailedWaveAcknowledgesIt` avoids on
purpose.

### 4. Acknowledge by identity

Like option 3, but acknowledge only the listed waves whose stored error is the same value as the cancellation cause.
Narrower, but it depends on error identity, and a wave whose abandonment cause equals the poison would be
acknowledged too. More subtle for little gain.

## Recommendation (superseded by the decision below)

Option 3. It closes the gap, keeps the admission order and the `TryMoveTo` contract, and reuses a rule the runtime
has in three places. Option 2 is a defensible design but a larger change with a new `MoveTo` / `TryMoveTo`
asymmetry, not worth it for this case alone.

## Decision

Remove the `joins ...Wave` parameter from `MoveTo` and `TryMoveTo` on both `Stage` and `FanOut`, and add
`Wait(ctx) error` to `Wave`. The processor waits for a wave where it wants to: before a move, holding the previous
node's slot, or after it, holding the target's slot. `Detach` and `Retain` stay as they are.

Why this instead of the options above:

- The gap is not specific to fan-outs. A `Retain` wave fails the same way: `runRetain` poisons the item, and a
  `MoveTo` blocked on admission returns the cause without reaching the join. A fix must live on the wave.
- Option 2 versus the current order is a choice of which downstream slot the item holds while it waits. The wave
  keeps the fan-out's slot (or the retained stage's slot) until its work is done and the item has moved past that
  node, in both orders. What differs is the extra slot the item itself occupies: the previous node's, the target's,
  or the target's waiting room if the item had stepped into it. Which of those is scarce depends on the pipeline;
  the library cannot know. A blocking wait hidden inside `MoveTo` forces one answer on every user.
- A wait on the wave itself has no admission wait in front of it, so case B cannot happen: the wake-up on poison
  lands in the wait that owns the wave. A finished failed wave is always acknowledged by the call that reports it.
- `TryMoveTo` loses its "joins are awaited only if the item entered" clause. `MoveTo` does one thing. Error
  wrapping becomes uniform: a wave error carries its node name from `joinedErr` and nothing about the move.
- It answers open question 2 of the design (`Wait(ctx, joins ...Wave)` on the fan-out) with a method on the object
  that owns the state, which also serves `Retain` waves.

Compatibility: breaking, accepted. v0.10.0 already changes the `MoveTo` signature, so this is the right release for
it. Migration is mechanical (see below).

### Behavior

`Wave.Wait(ctx) error` blocks until the wave is finished and reports its outcome. Only the item that created the wave
may call it.

1. Caller resolution, in this order. A standalone wave (returned by a `Detach` or `Retain` whose preamble failed: a
   foreign or stale context) has no run and no item; it returns its stored error at once, with no further checks. A
   pool work's context panics `errCannotMove`, as `FanOut.Wait` does: a task must never wait for other work. A context
   that carries no item returns `ErrForeignContext`. A context whose item is not the wave's item panics
   `errForeignWave`. This covers another item, another conveyor, and a lane child waiting on its parent's wave (a
   child is its own item; it may wait only on waves it created, and its parent's body wave cannot finish before the
   child completes). Then the run's lock is taken; a finished item returns `ErrStaleContext` (normally its canceled context is
   caught first by the cancellation check; this catches a caller that stripped cancellation from its context).
2. Wait with `waitUntil(ctx, it, w.isFinished)`, which tests cancellation before the condition, also on entry, and
   wakes on the call context or the item's canonical context. The item's own cause wins over the call context's. The
   lock is released while waiting and held again on return.
3. On a cancellation wake-up: a wave that is finished with an error is acknowledged and its error is returned. A wave
   still running, or finished clean, returns the cancellation cause and acknowledges nothing. A canceled item never
   gets nil. The error precedence (a finished failed wave beats the cancellation cause) is the one `run.join`,
   `FanOut.Wait` and `joinPending` apply today; their readiness conditions differ (`FanOut.Wait` waits for an idle
   open body, `joinPending` seals and acknowledges an idle clean body too).
4. On a normal wake-up: acknowledge; return the wave's error, or nil.
5. Error naming. `joinedErr` names the node the work belongs to: today only a fan-out body (`atNode`). It gets a
   fallback to `retainUnit`, so a `Retain` wave reads `<stage> work: <err>` and a detached fan-out wave reads
   `<fan-out> work: <err>`. A wave with neither (the finished wave a canceled `Retain` hands back) returns its error
   raw. The completion path (`firstUnackedWaveErr`) keeps returning raw errors, unchanged.
6. Repeated calls. Rule 1 applies to every call: once the item has finished, `Wait` returns `ErrStaleContext`.
   Once the wave is finished with an error, every valid call returns that error. A wave finished clean returns nil, or
   the cancellation cause if the item or the call context is canceled (a derived deadline included). Before the wave
   is finished, a call woken by cancellation returns the cause and leaves the wave unacknowledged; a call after
   `Finished` closes, or `Err` after `Finished`, then reports and acknowledges the wave's error.
7. The lock discipline is the one of `run.join`: everything from the stale check to the acknowledgement happens under
   `run.mu`. The implementation reads `w.err` directly and must not call the public `Err`, which takes the same lock.

Acknowledgement paths after the change: `Wave.Wait` (rule 3 and 4), `Err` called after `Finished` is closed,
`FanOut.Wait` on the open body, and the leave of a fan-out (`joinPending`). `Finished` alone never acknowledges. Item
completion is unchanged: the item waits for every wave, and when the processor returned nil or a `ShutdownError`, the
first unacknowledged error in wave creation order fails the item. Acknowledging one wave says nothing about the others
and does not undo the item's cancellation.

`MoveTo(ctx)` and `TryMoveTo(ctx)` no longer take or look at listed waves. The item's own open body is unchanged: a
leave still waits for it, seals it and acknowledges it (§6.4), and `TryMoveTo` still declines while the body is busy.

Where the item stands during the wait is the processor's choice:

```go
w := crawl.Detach(ctx)
if err := report.MoveTo(ctx); err != nil { return err }
// ...
// (a) hold report's slot while waiting, then take commit
if err := w.Wait(ctx); err != nil { return err }
if err := commit.MoveTo(ctx); err != nil { return err }
// (b) take commit first, wait there
if err := commit.MoveTo(ctx); err != nil { return err }
if err := w.Wait(ctx); err != nil { return err }
```

Waiting inside a fan-out whose door is still closed (entered, nothing scheduled yet) keeps the items behind out of
the fan-out for the duration, as a join at `MoveTo` does today. Documented, not prevented.

A `Retain` callback receives no context of its own and can capture the processor's context, so a context check
cannot stop it from waiting on its own wave, which cannot finish before the callback returns. Such a `Wait` ends only
through cancellation. This hazard exists today with `Finished` and is unchanged.

What the change gives for the trap in the "Problem" section: the task fails while `commit.MoveTo(ctx)` blocks on
admission; `MoveTo` returns the cause; the processor calls `w.Wait(ctx)`. If the wave is finished, `Wait` returns
its error and acknowledges it, and returning nil ends the item clean, with a free or a busy slot alike. If sibling
tasks or sources of the wave are still winding down, `Wait` returns the cause and acknowledges nothing, and the
processor that wants a clean completion waits for `Finished` and reads `Err`, or calls `Wait` again after `Finished`
closes (a retry on a canceled item with an unfinished wave returns the cause again at once). The old
timing dependency, an admission wait that swallows the wake-up of a finished failed wave, is gone. The remaining
one, a failed wave that has not finished yet, is visible to the processor and has a documented answer.

### Implementation steps

Runtime:

- [x] `wave.go`: add `Wait(ctx context.Context) error` to the `Wave` interface with the contract above. Implement it
      on `*wave` with the caller resolution of rule 1 (a new small preamble: standalone check, pool-work marker,
      `itemFromContext`, identity against `w.it`, lock, `finished` check), then the body of today's `run.join` for one
      wave. A nil `*wave` receiver panics `errForeignWave`; a nil `Wave` interface value cannot be caught.
- [x] `wave.go`: delete `run.join`. Keep `errForeignWave` and update its comment in `errors.go` (now panicked by
      `Wave.Wait`). Update the `wave.acked` comment (observed by `Wait`, by `Err` after `Finished`, by `FanOut.Wait`,
      by the leave).
- [x] `state.go`: `joinedErr` falls back to `retainUnit` when `atNode` is nil (rule 5).
- [x] `stage.go`, `fanout.go`: drop `joins ...Wave` from `MoveTo` and `TryMoveTo` (interface and implementation),
      remove the `join at <node>` calls and wording from the doc comments. `TryMoveTo` doc: remove the "joins are
      awaited only if the item entered" sentence. `Stage.Retain` and `FanOut.Detach` docs: "join in a later MoveTo"
      becomes "wait for it with `Wave.Wait`".
- [x] `context.go`: the `actingItem` comment refers to `run.join` returning under a held lock; remove the comparison
      (`Wave.Wait` releases the lock before it returns).
- [x] `wave.go` header comment and `conveyor.go:18`: "joined with a later MoveTo" becomes "waited for with
      `Wave.Wait`, or read through `Finished` and `Err`".

Tests:

- [x] Migrate every `X.MoveTo(ctx, w...)` / `X.TryMoveTo(ctx, w...)` call site to a move followed by `w.Wait(ctx)`
      (for `TryMoveTo`, only after `entered == true`): `detach_test.go`, `errors_test.go`, `fanout_test.go`,
      `retain_test.go`, `trymoveto_test.go`, `wave_test.go`, `schedule_test.go`, `interaction_test.go`
      (`TestRetainReleasesWhileItemSitsInFanOut`, `TestRetainedStageFillsItsWaitingRoom`), `cancel_test.go`
      (`TestStrippedContextJoinWaitWakesOnItemCancellation`, which must still cancel during the wave wait), and the
      `mover` interface plus its fallback helper in `property_test.go`.
- [x] `TestTryMoveToReportsEnteredWithJoinError`, `TestTryMoveToFanOutJoinErrorPoisonsBody`: rewrite as "entered,
      then `Wait` fails"; the entered flag and the wave error are now separate results.
- [x] `TestErrForeignWaveFromNilWave`: retire, or redefine for a typed nil `*wave` receiver.
- [x] `TestJoinSeveralWavesReportsFirstFailure`: synchronize the creation and failure of both waves; `Wait` in order
      returns the first failing wave's error; the second wave stays unacknowledged until waited for; the run then
      ends clean only after both were waited for.
- [x] `TestJoinForeignWavePanics`: `Wait` with another item's context panics `errForeignWave`; add a pool-work
      context case (panics `errCannotMove`), a context without an item (`ErrForeignContext`), a stripped context of a
      finished item (`ErrStaleContext`), and a lane child: its own `Retain` wave is allowed, its parent's wave panics.
- [x] `TestJoinOfFailedWaveAcknowledgesIt`: `Wait` woken by the poison on a finished wave returns the wave's error
      and acknowledges it.
- [x] New test for the case this issue is about: `commit` limit 1 held by another item, the single task fails while
      the item blocks in `commit.MoveTo(ctx)`, `MoveTo` returns the cause, `w.Wait(ctx)` returns the wave error, the
      processor returns nil, the run ends clean. Same test for a `Retain` wave, asserting the `<stage> work:` prefix.
- [x] New test: a failed wave that is not finished yet (a sibling task still running) makes `Wait` return the cause
      unacknowledged; after `Finished`, a second `Wait` returns the wave error and acknowledges it; with a second
      failed wave left unacknowledged the item still fails at completion.
- [x] New test: `Wait` before the move holds the previous node's slot (the item behind is kept out of the previous
      node, and the target stays free) and `Wait` after the move holds the target's slot.
- [x] New test: `Wait` on a standalone wave returns its error without a caller check; `Wait` on a clean finished wave
      of a canceled item returns the cause; a derived deadline on the call context ends `Wait` with that deadline.
- [x] Remove tests that only asserted that a `MoveTo` with joins acknowledges nothing when admission fails.

Documentation and migration:

- [x] `design_fanout_schedule.md`: §4.1 signature, §5.1 (`MoveTo`, `TryMoveTo`, `Detach` doc text), §5.4
      (`Wave.Wait`), §6.1 step 7 (remove; `TryMoveTo` paragraph), §6.4 `TryMoveTo` step 4 ("publish, join"), §6.7
      acknowledgement list, §6.10 and §6.11 (who may wait on a wave; the panics), §8.8, §9 breaking changes, §10
      question 2 (answered), §12 release notes and migration table. Add §11.14 phase 12 with these steps, and a change
      log entry (revision 8) pointing at this file.
- [x] `impl_details.md`: the acknowledgement and join description (around line 373).
- [x] `docs/4_fan-out.md` (Detach section, `commit.MoveTo(ctx, wave)`), `docs/1_retain-previous-stage.md`,
      `docs/README.md`, `docs/6_conditional-move-to.md`, `docs/9_design_faq.md`, `README.md`: replace joins-in-`MoveTo`
      with `Wave.Wait`, add one sentence on choosing where to wait, and one on `Finished` plus `Err` for a wave that
      may still be winding down after a failure.
- [x] `bench/internal/pipeline/conveyor.go`: migrate the `Retain` / `Detach` call sites. Check `demo` for the same.
- [x] Migration note: `commit.MoveTo(ctx, w1, w2)` becomes `commit.MoveTo(ctx)` followed by `w1.Wait(ctx)` and
      `w2.Wait(ctx)` for the old placement; put the waits before the move to hold the previous slot instead. A
      `Retain` wave's error now carries the stage name.

Verification:

- [x] `gofmt -l .` is empty; `go vet ./...` is clean.
- [x] `go test -race ./...` passes, including the property suite.
- [x] `bench` and `demo` are separate modules: run `go vet ./... && go test -race ./...` in each (the bench has a
      behavioral equivalence test in `internal/pipeline`); the demo binary is `js && wasm` only, so also
      `GOOS=js GOARCH=wasm go build ./...` in `demo`.
- [x] `grep -rn 'joins' *.go docs README.md design_fanout_schedule.md impl_details.md` finds no leftover mention of
      the parameter.
