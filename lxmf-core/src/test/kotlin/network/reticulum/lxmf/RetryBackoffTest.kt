package network.reticulum.lxmf

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

/**
 * The retry curve, asserted directly.
 *
 * The curve is standard across the clients we ship, so the values are not free to drift: two
 * devices sending to the same absent peer should give up asking at the same rate, and a difference
 * here is the kind that shows up as one of them "being slower" with no visible cause.
 */
class RetryBackoffTest {

    @Test
    fun `afterStep should start at the base wait`() {
        // then
        // The first retry is deliberately close: most first failures are a carrier that came up a
        // moment too late, and waiting the ten seconds Python waits is most of a short exchange.
        assertEquals(2_000L, RetryBackoff.afterStep(0))
    }

    @Test
    fun `afterStep should double each step`() {
        // then
        assertEquals(4_000L, RetryBackoff.afterStep(1))
        assertEquals(8_000L, RetryBackoff.afterStep(2))
        assertEquals(16_000L, RetryBackoff.afterStep(3))
    }

    @Test
    fun `afterStep should stop at the cap`() {
        // then
        // Doubling reaches the cap between step 7 and step 8; past that the answer never changes,
        // which is what stops a long-absent peer from being polled forever at a shrinking interval.
        assertTrue(RetryBackoff.afterStep(7) < RetryBackoff.CAP_MILLIS)
        assertEquals(RetryBackoff.CAP_MILLIS, RetryBackoff.afterStep(8))
        assertEquals(RetryBackoff.CAP_MILLIS, RetryBackoff.afterStep(9))
    }

    @Test
    fun `afterStep should hold the cap for any step beyond the doubling range`() {
        // then
        // The guard against a shift that is no longer arithmetic. A step this large means the
        // message has been retried more times than the cap needed to decide the answer.
        assertEquals(RetryBackoff.CAP_MILLIS, RetryBackoff.afterStep(64))
        assertEquals(RetryBackoff.CAP_MILLIS, RetryBackoff.afterStep(Int.MAX_VALUE))
    }

    @Test
    fun `afterStep should treat a negative step as the first one`() {
        // then
        // Nothing should produce a negative step, but a wait computed from one would be a shift by
        // a negative amount — a number that is not a delay at all rather than merely a wrong one.
        assertEquals(RetryBackoff.afterStep(0), RetryBackoff.afterStep(-1))
        assertEquals(RetryBackoff.afterStep(0), RetryBackoff.afterStep(Int.MIN_VALUE))
    }
}
