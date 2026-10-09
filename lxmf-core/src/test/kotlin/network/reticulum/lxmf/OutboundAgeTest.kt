package network.reticulum.lxmf

import kotlinx.coroutines.runBlocking
import network.reticulum.identity.Identity
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

/**
 * [LXMRouter.MAX_OUTBOUND_AGE] counts from the moment a message was handed to
 * [LXMRouter.handleOutbound], not from its timestamp.
 *
 * A caller that keeps a message over a restart hands it over again with its original timestamp,
 * so the hash stays the same. Measured from the timestamp, a message older than a day was failed
 * on its first pass after the restart; measured from hand-over, it gets a fresh day.
 */
@DisplayName("LXMF outbound age limit counts from hand-over")
class OutboundAgeTest : OutboundQueueTestBase() {

    @Test
    fun `a message with a two-day-old timestamp handed over now is still sent`() = runBlocking {
        val dest = deliveryDestination(Identity.create())
        val twoDaysAgo = System.currentTimeMillis() / 1000.0 - TWO_DAYS_IN_SECONDS
        val message = messageTo(dest, "kept over a restart", timestamp = twoDaysAgo)
        val failures = AtomicInteger()
        router.registerFailedDeliveryCallback { failures.incrementAndGet() }

        router.handleOutbound(message)
        driveUntil { message.nextDeliveryAttempt != null }
        driveAFewPasses()

        assertEquals(twoDaysAgo, message.timestamp, "the timestamp is not touched")
        assertEquals(MessageState.OUTBOUND, message.state)
        assertEquals(1, router.pendingOutboundCount())
        assertEquals(0, failures.get())
        val handedOverAt = assertNotNull(message.handedOverAt)
        assertTrue(System.currentTimeMillis() - handedOverAt < 5_000, "the hand-over time is now")
    }

    @Test
    fun `a message handed over more than a day ago is failed`() = runBlocking {
        val dest = deliveryDestination(Identity.create())
        val message = messageTo(dest, "forgotten")
        val failures = AtomicInteger()
        router.registerFailedDeliveryCallback { failures.incrementAndGet() }
        router.handleOutbound(message)

        message.handedOverAt = System.currentTimeMillis() - LXMRouter.MAX_OUTBOUND_AGE - 1
        driveUntil { message.state == MessageState.FAILED }

        assertEquals(0, router.pendingOutboundCount())
        assertEquals(1, router.failedOutboundCount())
        assertEquals(1, failures.get())
    }

    @Test
    fun `handing a message over again gives it a fresh day`() = runBlocking {
        val dest = deliveryDestination(Identity.create())
        val message = messageTo(dest, "once more")
        val failures = AtomicInteger()
        router.registerFailedDeliveryCallback { failures.incrementAndGet() }
        router.handleOutbound(message)
        message.handedOverAt = System.currentTimeMillis() - LXMRouter.MAX_OUTBOUND_AGE - 1
        driveUntil { message.state == MessageState.FAILED }

        router.handleOutbound(message)
        driveAFewPasses()

        assertEquals(MessageState.OUTBOUND, message.state)
        assertEquals(1, router.pendingOutboundCount())
        assertEquals(1, failures.get(), "the second hand-over did not fail again")
    }

    private companion object {
        const val TWO_DAYS_IN_SECONDS = 2 * 24 * 60 * 60
    }
}
