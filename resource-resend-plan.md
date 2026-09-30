# Plan: the resend storm, and replies over links that are not backchannels

Written on 2026-09-30, after a Pixel 5 running the Analog app crashed with `OutOfMemoryError`. The
decisions were taken by the developer the same day, and part 3 was revised after reading Python's
`LXMF/LXMRouter.py`.

Line numbers are at this fork's tag `0.1.0`, reticulum-kt's fork at `0.1.0`, Python LXMF at
`master` (read on 2026-09-30), LXMF-swift at `c2ab34b` (0.8.0) and reticulum-swift at `4b88677`
(0.6.0), which are what the iOS app pins.

## What goes wrong

Three things add up. Each is harmless alone; together they grow threads without limit.

1. **This fork replies over links that are not backchannels.** In Python, a link becomes a
   backchannel only when its opener identifies on it (`delivery_remote_identified`,
   `LXMRouter.py:2060–2062`). The opener does that after delivering a message over a link it
   opened, and in the same step calls `delivery_link_established` on it, so the link accepts
   Resources from then on (`:2763–2776`). A Python backchannel therefore always accepts Resources.
   This fork also stores any link a DIRECT message arrived on as a backchannel, identified or not
   (`processInboundDelivery`, `LXMRouter.kt:2146–2148`, from upstream's first commit `25d811e`).
   LXMF-swift never identifies on the links it opens (`LXMRouter+Delivery.swift:288–310`), and
   those links keep reticulum-swift's default `.acceptNone` (`Link.swift:254`), under which an
   advertisement is dropped without a reply (`Link.swift:2110–2113`). So when this fork has no
   active link of its own (`processDirectDelivery`, `:1313–1320`), a message too big for one
   packet goes as a Resource over the iPhone's link (`sendViaLink`, `:1847`), and the iPhone
   ignores it. The Resource times out after its 16 retries (about 17 s), and
   `retryAfterUnproven` (`:1104`) schedules the message again. The retry model has no end but
   `MAX_OUTBOUND_AGE`, so this repeats for as long as the link lives.
2. **The stall rule resends on every pass.** The `SENDING` branch of `processOutbound`
   (`:817–835`) resends a message whose Resource has sat `SENDING_STALL_TIMEOUT` (120 s) past
   `nextDeliveryAttempt`. Neither the rule nor `sendViaLink` moves `nextDeliveryAttempt`, so once
   the rule fires it fires again on every pass, about once a second. It does not count the resend
   in `deliveryAttempts` either, so `MAX_DELIVERY_ATTEMPTS` never stops it.
3. **A resend does not cancel the Resource it replaces.** `sendViaLink` overwrites the message's
   entry in `pendingResources` (`:1916`) and leaves the old Resource alive. On a link that is busy
   with another Resource, reticulum-kt parks each new one in `QUEUED` with a thread of its own
   that polls every 250 ms until the link is free (`Resource.advertise`, `Resource.kt:561`).
   Nothing cancels the replaced ones, so those threads pile up. When a replaced Resource finally
   fails, its callback reschedules the message once more.

## Evidence

- **Pixel 5, 2026-09-29, 16:49–21:37.** The loop to an iPhone contact began at 16:54:34. The
  log shows 7,151 "stalled; sending it again", 505 Resources timed out, and only 512 ever reached
  the link. At 21:37:49 `pthread_create` failed with `OutOfMemoryError` in
  `Link.resourceConcluded`, and the app crashed.
- **Pixel 8 Pro, 2026-09-30.** 1,365 `resource-advertise-*` threads after 1.5 hours. Every
  retry in the part of its log still on the phone was for one iPhone contact. That iPhone's own
  log (Analog Dev 0.53.0) shows the Pixel's small commands arriving all morning, and no trace of
  the Resources.

## Decisions

Taken by the developer on 2026-09-30.

| Decision | Chosen |
|---|---|
| Which link a reply may use | Python's rule: a link is a backchannel only once its remote has identified on it. The fork's storing of any link a message arrived on goes. Chosen after reading Python; it replaced the first choice, "Resources only over a link this phone opened", the same day |
| A stall resend | Counts as a delivery attempt. After `MAX_DELIVERY_ATTEMPTS` (8) unanswered sends, `parkOnExhaustedRoute` (`:1004`) parks the message and forgets the route, instead of resending forever |
| Where this plan lives | Here, beside `port-deviations.md` |
| The iPhone side | Not an iOS bug. iPhones never identify on the links they open, so under Python's rule they offer no backchannel. Bug 8 on the Analog "Bugs and changes" page was withdrawn and replaced by a note under "Other findings" |

## Design

### 1. One live Resource per message

In `sendViaLink`:

- Before a new Resource starts for a message, cancel the one `pendingResources` holds for it, if
  any. A cancelled `QUEUED` Resource leaves its polling loop, so its thread ends.
- Every Resource send sets `nextDeliveryAttempt` to the moment of the send, so the stall rule
  measures from the latest send. Today a first send leaves it unset, and a later one leaves it
  where the last failure put it.
- A Resource's callbacks act only while it is still the message's entry in `pendingResources`.
  The completion or failure of a replaced Resource changes nothing.

### 2. The stall rule fires once per stall

In the `SENDING` branch of `processOutbound`:

- Cancel the stalled Resource before resending, as in 1.
- Count the resend in `deliveryAttempts`. `processDirectDelivery` already parks the message at
  `MAX_DELIVERY_ATTEMPTS` (`:1294–1297`), so a peer that never answers stops the message after
  8 sends rather than never.
- The send itself moves `nextDeliveryAttempt` (1), so the rule next fires 120 s after this send,
  not on the next pass.

### 3. A backchannel only after identification

In `processInboundDelivery` (`:2144–2160`):

- Stop storing the link a DIRECT message arrived on as a backchannel. Keep what the block still
  needs for an identified link, such as remembering the identity for `Identity.recall`.
- The remote-identified callback (`:1980–1992`) stays as the one place a backchannel is stored,
  as `delivery_remote_identified` is in Python.
- An iPhone's link is then never a backchannel, so a reply to an iPhone opens a link of this
  phone's own, for small and large messages alike.
- Android to Android is unchanged: this fork identifies on the links it opens as soon as they are
  up (`identifyOnLink`, `:1523`) and sets `ACCEPT_APP` on them (`:1601`).
- This removes a departure from Python, so `port-deviations.md` needs no entry for it.

### Not changed

- reticulum-kt: a thread per `QUEUED` Resource matches Python's `__advertise_job`, and with 1
  and 2 those threads end.
- `SENDING_STALL_TIMEOUT` (120 s), `MAX_DELIVERY_ATTEMPTS` (8), `MAX_OUTBOUND_AGE` and the
  parking rule keep their values.

## Tests

Beside `LXMRouterTest.kt`, in this repository's own style:

- a resend cancels the message's previous Resource;
- a replaced Resource's failure does not reschedule the message, and its completion does not
  mark it delivered;
- the stall rule resends once per `SENDING_STALL_TIMEOUT`, not on every pass;
- stall resends count toward `MAX_DELIVERY_ATTEMPTS`, and the message parks after 8;
- a DIRECT message arriving over a link whose remote has not identified does not make that link a
  backchannel: a reply to that sender opens a link of its own and sends nothing on it;
- a link whose remote identified is a backchannel as before, and a reply may use it, a Resource
  included.

Where a rule cannot be reached without a live link, say so and propose the smallest seam rather
than skipping the test.

## Release

1. Tag this fork after `0.1.0`. JitPack builds `com.github.kishontivf:LXMF-kt` from the tag.
2. In intercom-kt, set `lxmf` to the new tag and release intercom-kt to GitHub Packages.
3. In Analog, bump `net.kishonti:intercom` to that release.

To try it on phones before tagging, Analog has to reach a local LXMF-kt. Its
`settings.gradle.kts` admits only `net.kishonti` SNAPSHOT versions from `mavenLocal`, so that
filter needs widening for the test, or a composite build.

## Checked on phones

Each run needs the developer's go-ahead at that moment.

- An Android phone replies to an iPhone with a message too big for one packet, while only the
  iPhone's link is open: the Android phone opens its own link, and the message arrives.
- A peer that never answers: the message parks after 8 sends, and the app's thread count stays
  flat.

## Open points

- The product changes from `7b0a417` (the retry model with no end but `MAX_OUTBOUND_AGE`, the
  stall rule, parking) are not in `port-deviations.md`, although its rule says every deviation
  must be. Out of scope here; worth entries of their own.
