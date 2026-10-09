package network.reticulum.lxmf

import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import network.reticulum.common.toHexString
import network.reticulum.identity.Identity
import network.reticulum.resource.ResourceConstants
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * [LXMRouter.cancelOutbound] takes one queued instance out of the queue for good.
 *
 * It cancels the instance it is given, not every instance with its hash; it returns whether the
 * instance was still queued; it cancels the instance's Resource; and none of the paths that put a
 * message back on the queue — a failed attempt, the resend of an unproven packet, a closed link,
 * the stall rule — may bring a cancelled instance back. Proofs for packets already in flight are
 * in [CancelOutboundReceiptTest], which needs a started Transport.
 */
@DisplayName("LXMRouter.cancelOutbound")
class CancelOutboundTest : OutboundQueueTestBase() {

    /** Calls the private [LXMRouter.retryAfterUnproven], as a receipt timeout or a failed Resource does. */
    private fun retryAfterUnproven(message: LXMessage) {
        val method = LXMRouter::class.java.getDeclaredMethod("retryAfterUnproven", LXMessage::class.java, String::class.java)
        method.isAccessible = true
        method.invoke(router, message, "a test attempt went unproven")
    }

    /** Calls the private [LXMRouter.handleLinkClosed], as a delivery link's closed callback does. */
    private fun handleLinkClosed(destHashHex: String) {
        val method = LXMRouter::class.java.getDeclaredMethod("handleLinkClosed", String::class.java)
        method.isAccessible = true
        method.invoke(router, destHashHex)
    }

    @Test
    fun `cancelOutbound removes the queued instance and returns true`() = runBlocking {
        val dest = deliveryDestination(Identity.create())
        val message = messageTo(dest, "hello")
        val failures = AtomicInteger()
        val routerFailures = AtomicInteger()
        message.failedCallback = { failures.incrementAndGet() }
        router.registerFailedDeliveryCallback { routerFailures.incrementAndGet() }
        router.handleOutbound(message)

        assertTrue(router.cancelOutbound(message))

        assertEquals(MessageState.CANCELLED, message.state)
        assertEquals(0, router.pendingOutboundCount())

        driveAFewPasses()

        assertEquals(MessageState.CANCELLED, message.state)
        assertEquals(0, failures.get(), "no callback fires for a cancelled instance")
        assertEquals(0, routerFailures.get())
        assertEquals(0, router.failedOutboundCount())
    }

    @Test
    fun `cancelOutbound returns false for an instance the router does not hold`() = runBlocking {
        val dest = deliveryDestination(Identity.create())
        val message = messageTo(dest, "never queued")
        message.pack()

        assertFalse(router.cancelOutbound(message))
        assertEquals(MessageState.GENERATING, message.state, "an unknown instance is left alone")

        router.handleOutbound(message)

        assertTrue(router.cancelOutbound(message))
        assertFalse(router.cancelOutbound(message), "a second cancel finds nothing to cancel")
    }

    @Test
    fun `cancelOutbound cancels only the instance it is given, not the other one with its hash`() = runBlocking {
        val dest = deliveryDestination(Identity.create())
        val timestamp = System.currentTimeMillis() / 1000.0
        val direct = messageTo(dest, "hello", DeliveryMethod.DIRECT, timestamp)
        val propagated = messageTo(dest, "hello", DeliveryMethod.DIRECT, timestamp)
        router.handleOutbound(direct)
        router.handleOutbound(propagated)
        assertTrue(direct.hash.contentEquals(propagated.hash))

        assertTrue(router.cancelOutbound(direct))

        assertEquals(1, router.pendingOutboundCount())
        assertEquals(MessageState.CANCELLED, direct.state)
        assertEquals(MessageState.OUTBOUND, propagated.state)

        assertTrue(router.cancelOutbound(propagated))

        assertEquals(0, router.pendingOutboundCount())
    }

    @Test
    fun `cancelOutbound cancels the instance's Resource in flight`() = runBlocking {
        val dest = deliveryDestination(Identity.create())
        val message = messageTo(dest, resourceSizedContent)
        val failures = AtomicInteger()
        message.failedCallback = { failures.incrementAndGet() }
        val resource = sendAsResource(message, dest)

        assertTrue(router.cancelOutbound(message))

        assertEquals(ResourceConstants.FAILED, resource.status, "the Resource is cancelled")
        assertNull(router.pendingResourceForTest(message))
        assertEquals(MessageState.CANCELLED, message.state)
        // Cancelling the Resource fired its failed callback, which found it was no longer the
        // instance's entry and did nothing.
        assertEquals(0, failures.get())
    }

    @Test
    fun `a failed attempt does not put a cancelled instance back`() = runBlocking {
        val dest = deliveryDestination(Identity.create())
        val message = messageTo(dest, "hello")
        val failures = AtomicInteger()
        message.failedCallback = { failures.incrementAndGet() }
        router.handleOutbound(message)
        assertTrue(router.cancelOutbound(message))

        retryAfterUnproven(message)
        delay(200)

        assertEquals(MessageState.CANCELLED, message.state)
        assertEquals(0, router.pendingOutboundCount())
        assertEquals(0, failures.get(), "the failed callback stays silent for a cancelled instance")
    }

    @Test
    fun `a failed attempt still puts a queued instance back`() = runBlocking {
        val dest = deliveryDestination(Identity.create())
        val message = messageTo(dest, "hello")
        val failures = AtomicInteger()
        message.failedCallback = { failures.incrementAndGet() }
        router.handleOutbound(message)
        driveUntil { message.nextDeliveryAttempt != null }
        message.state = MessageState.SENT

        retryAfterUnproven(message)
        driveUntil { message.state == MessageState.OUTBOUND }

        assertEquals(1, failures.get())
        assertEquals(1, router.pendingOutboundCount())
    }

    @Test
    fun `the unproven-packet resend does not send a cancelled instance again`() = runBlocking {
        val dest = deliveryDestination(Identity.create())
        val message = messageTo(dest, "hello")
        router.handleOutbound(message)
        // A packet that went out and was never proven: due for its resend.
        message.state = MessageState.SENT
        message.method = DeliveryMethod.DIRECT
        message.nextDeliveryAttempt = 0L
        assertTrue(router.cancelOutbound(message))
        val dispatched = CopyOnWriteArrayList<LXMessage>()
        router.testHookOnProcessOutboundMessage = { dispatched.add(it) }

        try {
            driveAFewPasses()
        } finally {
            router.testHookOnProcessOutboundMessage = null
        }

        assertTrue(dispatched.none { it === message }, "a cancelled instance is never dispatched")
        assertEquals(MessageState.CANCELLED, message.state)
    }

    @Test
    fun `a closed link does not put a cancelled instance back`() = runBlocking {
        val dest = deliveryDestination(Identity.create())
        val message = messageTo(dest, resourceSizedContent)
        sendAsResource(message, dest)
        assertTrue(router.cancelOutbound(message))

        handleLinkClosed(dest.hash.toHexString())
        delay(200)

        assertEquals(MessageState.CANCELLED, message.state)
        assertEquals(0, router.pendingOutboundCount())
    }

    @Test
    fun `the stall rule does not send a cancelled instance again`() = runBlocking {
        val dest = deliveryDestination(Identity.create())
        val message = messageTo(dest, resourceSizedContent)
        sendAsResource(message, dest)
        assertTrue(router.cancelOutbound(message))
        stall(message)
        val dispatched = CopyOnWriteArrayList<LXMessage>()
        router.testHookOnProcessOutboundMessage = { dispatched.add(it) }

        try {
            driveAFewPasses()
        } finally {
            router.testHookOnProcessOutboundMessage = null
        }

        assertTrue(dispatched.none { it === message })
        assertEquals(MessageState.CANCELLED, message.state)
        assertNull(router.pendingResourceForTest(message), "no new Resource is started")
    }

    @Test
    fun `cancelOutbound takes a message that is waiting for its deferred stamp`() = runBlocking {
        val dest = deliveryDestination(Identity.create())
        val message = messageTo(dest, "stamped later")
        message.deferStamp = true
        message.stampCost = 1
        router.handleOutbound(message)
        assertEquals(0, router.pendingOutboundCount(), "a deferred message is not in the queue yet")

        assertTrue(router.cancelOutbound(message))

        // The processing loop makes deferred stamps; a cancelled message never reaches the queue.
        router.start()
        delay(DEFERRED_STAMP_WAIT)

        assertEquals(MessageState.CANCELLED, message.state)
        assertEquals(0, router.pendingOutboundCount())
    }

    private companion object {
        /** Longer than one processing interval, so the loop had its chance at the deferred set. */
        const val DEFERRED_STAMP_WAIT = LXMRouter.PROCESSING_INTERVAL + 500L
    }
}
