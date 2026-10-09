# LXMF-kt — Documented Deviations from the Python Reference

This file is the **single source of truth** for every place where LXMF-kt's logic intentionally diverges from `markqvist/LXMF`. Any divergence not listed here is a bug, not a deviation.

## Rule

> All logic in LXMF-kt MUST mirror the python reference identically. Deviations are allowed ONLY for one of two reasons, both of which MUST be documented here before the code lands.

**Allowed reason 1 — Language/runtime forced.** The python pattern cannot be expressed faithfully in kotlin or on the JVM. Examples: coroutines vs threads, `@Volatile` vs the GIL, `ReentrantLock` where python relies on GIL-implicit serialization, `kotlinx.coroutines.runBlocking` boundaries at JVM/non-coroutine seams.

**Allowed reason 2 — New feature not present in python.** Kotlin-only API surface added for downstream consumers (Android lifecycle adapters, mobile-specific entry points, etc.). The kotlin-only behavior must not change semantics of any code path that *does* exist in python.

## Process

1. Before changing a kotlin port file in a way that diverges from the python reference, read the corresponding python source.
2. If the divergence is unavoidable for one of the two reasons above, add a section below using the template, then implement the change.
3. If you're unsure whether a divergence is justified, ask the human owner before picking unilaterally. Ports drift one small "harmless" choice at a time.
4. Reviewers should reject any PR that introduces a kotlin/python semantics divergence not represented in this file.

## Entry template

```markdown
### <short title> — <kotlin-file-relative-path>:<line-or-symbol>

**Python reference:** `<path>:<line>` (e.g. `LXMF/LXMRouter.py:2554-2580`)

**Category:** language/runtime forced  |  new feature

**Date:** YYYY-MM-DD

**Tracking:** issue/PR link, if any.

**Description:** what the kotlin code does, why it differs from python, and (for category 1) why no kotlin idiom can express the python semantics directly.

**Re-evaluation:** if a future kotlin/JVM/library change would make the python pattern expressible, what to look for.
```

---

## Deviations

### `@Volatile` on `LXMessage.progress` — `lxmf-core/src/main/kotlin/network/reticulum/lxmf/LXMessage.kt:142`

**Python reference:** `LXMF/LXMF/LXMessage.py:156` (`self.progress = 0.0`), with cross-thread writers at `LXMessage.py:474, 488, 496, 506, 512, 559, 571, 583, 618` (the `__update_transfer_progress` callback path) and reads from any caller polling for UI progress display.

**Category:** language/runtime forced

**Date:** 2026-05-12

**Tracking:** torlando-tech/LXMF-kt#34 (greptile review of `cmdLxmfGetMessageProgress`)

**Description:** Python's GIL serialises attribute reads/writes — `self.progress = X` from a Resource progress callback thread is implicitly visible to a main-thread poller without any explicit synchronisation. On the JVM, `var progress: Double = 0.0` has neither visibility nor atomicity guarantees: JLS §17.7 explicitly permits non-volatile `double` (and `long`) reads to **tear** (be observed as 32-bit halves of two different writes), and there is no happens-before edge between a write on one thread and a read on another without a synchronisation action. HotSpot makes 64-bit reads atomic in practice on modern hardware, but ART (Android Runtime) does not guarantee this, and visibility (vs atomicity) is implementation-defined either way. `@Volatile` is the direct JVM idiom for "what Python's GIL gives you for free": each read sees the latest committed write, and 64-bit access is guaranteed atomic.

Writers in this port: `LXMRouter.processOpportunisticDelivery` (LXMRouter.kt:739, 755) — `processingScope` coroutine; `LXMRouter.sendViaPropagation` Resource progressCallback (LXMRouter.kt:1258) — Resource background thread; `LXMRouter.sendViaLink` Resource progressCallback + completion callback (LXMRouter.kt:1335, 1340) — Resource background thread.

Readers: any consumer polling progress for UI display, plus `:conformance-bridge`'s `cmdLxmfGetMessageProgress` (Main.kt:740), which is what surfaced this issue in code review.

**Re-evaluation:** Remove `@Volatile` only if `LXMessage` ever migrates to an immutable / coroutine-`StateFlow`-backed progress representation, or if Kotlin gains a portable concurrency annotation that subsumes JVM-`@Volatile` semantics across all targets (Native, JS) the lib might one day support.

### DIRECT-link CLOSED-branch path re-request relocated to the link `closedCallback` — `lxmf-core/src/main/kotlin/network/reticulum/lxmf/LXMRouter.kt::establishLinkForMessage` (closedCallback) and `::processDirectDelivery` (CLOSED branch)

**Python reference:** `LXMF/LXMF/LXMRouter.py:2610-2629` — inside `process_outbound`'s per-message loop, when a message's DIRECT delivery link is `CLOSED`, Python re-requests the path (`RNS.Transport.request_path`), distinguishing "was active, closed unexpectedly" (`direct_link.activated_at != None`) from "never activated" (re-request once, guarded by the dynamic `path_request_retried` attribute), then pops both `direct_links` and `backchannel_links` and reschedules.

**Category:** language/runtime forced

**Date:** 2026-06-10

**Tracking:** columba#1004 (D2). See also `columba` memory `issue-1004-path-requests-direct-delivery`.

**Description:** Python's `direct_links` retains a CLOSED link until the next `process_outbound` tick observes it and runs the per-message CLOSED branch. The kotlin port is event-driven: `establishLinkForMessage` creates the `RNS.Link` with a `closedCallback` that fires the instant the link closes (Link watchdog establishment-timeout or unexpected teardown) and **eagerly removes** the link from `directLinks`, then calls `triggerProcessing()`. Consequently a CLOSED link is essentially never observed by `processDirectDelivery` — the next tick lands in the no-link branch — so porting Python's re-request into that branch would be dead code.

The re-request is therefore relocated to the `closedCallback`, where kotlin actually handles link close. It replicates Python's logic faithfully: `closedLink.activatedAt > 0` ⇒ re-request (was active); else re-request once gated by `LXMessage.pathRequestRetried` (never activated). It is additionally gated on the initiating message still needing delivery (`state == OUTBOUND || SENDING`) to reproduce the fact that Python's CLOSED branch only runs for a message still in the outbound loop — without this, a normal post-delivery close would emit a spurious path request that Python never makes. `processDirectDelivery`'s CLOSED branch is retained as a no-op-ish safety net (clear both link maps + reschedule) for the close-callback race window, but performs no re-request to avoid double-firing.

This matters specifically for transport-enabled nodes: reticulum-kt's `Transport.deregisterLink` stale-path recovery (expire + re-request on pending-link timeout) is intentionally gated to non-transport nodes (Python `Transport.py:504` parity), so for transport-mode users the LXMF close-time re-request is the only mechanism that refreshes a stale path after a failed DIRECT link.

**Re-evaluation:** If the kotlin `LXMRouter` ever stops eagerly removing the link in `closedCallback` and instead lets `processDirectDelivery` observe and pop CLOSED links (matching Python's `direct_links` lifecycle), move the re-request back into the CLOSED branch and delete this deviation. The per-message `pathRequestRetried` semantics would then align 1:1 with Python without the close-event approximation.

### Outbound state keyed by the queued instance, and `cancelOutbound(message)` — `lxmf-core/src/main/kotlin/network/reticulum/lxmf/LXMRouter.kt::pendingResources`, `::pathlessBackoffSteps`, `::awaitingFreshPath`, `::pendingDeferredStamps`, `::cancelOutbound`

**Python reference:** `LXMF/LXMF/LXMRouter.py::cancel_outbound(message_id)` and `pending_deferred_stamps`, both keyed by message id. Python tracks a message's resource on the message object itself (`LXMessage.resource_representation`).

**Category:** new feature

**Date:** 2026-10-08

**Tracking:** Analog FEATURE-25, milestone 1, step 0.

**Description:** A caller may queue one message twice under one hash: once direct or opportunistic, and once more as `PROPAGATED` when no proof came back in time. Everything the router keeps per message is therefore keyed by the queued `LXMessage` instance, never by its hash, so the two instances have their own Resource, their own route retry state and their own deferred stamp entry. `cancelOutbound(message: LXMessage): Boolean` cancels that one instance and returns whether it was still queued. It holds `outboundProcessingMutex` and `pendingOutboundMutex`, so no dispatch pass overlaps it. A receipt timeout and a Resource failure put a message back only while it is still queued (`retryAfterUnproven` checks that under the queue lock), and a closed link, the stall rule and the unproven-packet resend only ever look at queued instances. The one callback that still fires after a cancel is `deliveryCallback` from a proof of a packet already in flight, by the developer's decision. Python's `cancel_outbound` is keyed by message id and would cancel both instances.

**Re-evaluation:** None needed. If python ever gains a per-instance cancel, align the name.

### Outbound age counted from hand-over — `lxmf-core/src/main/kotlin/network/reticulum/lxmf/LXMRouter.kt::hasOutlivedTheQueue`, `LXMessage.handedOverAt`

**Python reference:** `LXMF/LXMF/LXMRouter.py::process_outbound` fails a message on `MAX_DELIVERY_ATTEMPTS`; it has no age bound.

**Category:** new feature

**Date:** 2026-10-08

**Tracking:** Analog FEATURE-25, milestone 1, step 0.

**Description:** This fork's only terminal bound, `MAX_OUTBOUND_AGE`, used to be measured from the message's timestamp. It is now measured from `LXMessage.handedOverAt`, which `handleOutbound` sets. A caller that keeps a message over a restart hands it over again with its original timestamp, so the hash is unchanged, and the message gets a fresh day instead of being failed on sight. The timestamp and the hash are not touched.

**Re-evaluation:** None needed; python has no age bound to align with.

### Propagation stamp made off the processing lock — `lxmf-core/src/main/kotlin/network/reticulum/lxmf/LXMRouter.kt::processPropagatedDelivery`, `::sendViaPropagation`, `::packForPropagation`

**Python reference:** `LXMF/LXMF/LXMessage.py::pack` (lines 434-441) makes the propagation stamp when the message is packed, on the caller's thread in `handle_outbound`; `process_outbound` only hands the packed bytes to a `Resource`.

**Category:** language/runtime forced

**Date:** 2026-10-08

**Tracking:** Analog FEATURE-25, milestone 1, step 0.

**Description:** `LXMessage.pack()` here is synchronous and the stamp is a suspending proof of work, so this port makes the stamp at send time. It used to do that inside `sendViaPropagation` with `runBlocking` while `outboundProcessingMutex` was held, which stopped every other send for the seconds a stamp takes on a phone. `processPropagatedDelivery` now sets the message `SENDING` and launches `sendViaPropagation` on `processingScope`; the stamp is made there under `stampGenMutex`, outside the processing lock. The Resource gets its callbacks and its `pendingResources` entry before it advertises, under the queue lock and only while the instance is still queued and `SENDING`, so a failure in that gap is not lost and a cancel during the stamp drops the upload unsent. The wire format is unchanged.

**Re-evaluation:** If `LXMessage.pack()` ever becomes suspending and makes the propagation stamp as python does, delete `packForPropagation` and this entry.

### `@Volatile` on `LXMessage.state` — `lxmf-core/src/main/kotlin/network/reticulum/lxmf/LXMessage.kt::state`

**Python reference:** `LXMF/LXMF/LXMessage.py` (`self.state`), written from the router loop, receipt callbacks and resource callbacks under the GIL.

**Category:** language/runtime forced

**Date:** 2026-10-08

**Tracking:** Analog FEATURE-25, milestone 1, step 0.

**Description:** The same reason as `progress` above. `state` is written by processing coroutines, receipt callbacks, Resource callbacks and now `cancelOutbound` from the caller's coroutine, and read by callers on other threads. `@Volatile` gives each read the latest write, which the GIL gives python for free.

**Re-evaluation:** As for `progress`.

### Stamped payload hashed over the received bytes — `lxmf-core/src/main/kotlin/network/reticulum/lxmf/LXMessage.kt::unpackFromBytes`, `::hashedPayload`

**Python reference:** `LXMF/LXMF/LXMessage.py::unpack_from_bytes` (lines 742-747). When the payload has a fifth element, python takes the first four elements, packs them again with msgpack, and hashes that.

**Category:** language/runtime forced

**Date:** 2026-10-08

**Tracking:** Analog FEATURE-25, milestone 2, step 2.

**Description:** This port used to decode the payload and encode the first four elements again, as python does. Python's round trip through msgpack gives back the bytes it was given for everything python itself writes. The JVM round trip does not. `fields` is a `Map<Int, Any>`, so a field with a nil value was dropped and the map shrank. msgpack-java decodes a float32 as a double, and the port encoded it as a float64. Either changes the bytes. The hash then no longer matches the one the sender signed, and a genuine signature fails to verify. `unpackFromBytes` now takes the received bytes, replaces the array header with a four-element header, and cuts everything from the stamp element on. That is the four elements exactly as the sender encoded them, which is what the sender hashed. A fields element that is msgpack nil is kept as the nil byte, which the old code reproduced by hand. A message python produced hashes to the same value as before, because python's packer writes the same element bytes with and without the stamp. LXMF-swift does the same in `hashedPayloadBytes` (commit `5605dd6`). One difference from python stays on purpose. A stamped message with a float32 field verifies here and in LXMF-swift. In python it fails, because python packs the float again as a float64. iOS writes its fields map in a random key order. A map decoded here keeps the wire order, so that case verified before too. `StampedPayloadHashTest` now pins it.

**Re-evaluation:** None needed. If python ever hashes the received bytes instead of packing them again, the two match exactly and this entry can go.
