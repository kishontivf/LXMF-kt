package network.reticulum.lxmf

import kotlinx.coroutines.runBlocking
import network.reticulum.destination.Destination
import network.reticulum.identity.Identity
import network.reticulum.resource.ResourceConstants
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNotSame
import kotlin.test.assertNull
import kotlin.test.assertSame
import kotlin.test.assertTrue

/**
 * The outbound queue tracks queued instances, not hashes.
 *
 * A caller queues one message twice under one hash: once direct, and again for a propagation node
 * when no proof came back in time. The router used to key a message's Resource and its retry
 * state by hash alone, so the node upload of a message over one link packet overwrote the direct
 * send's transfer, and the direct send never reported DELIVERED. These tests pin that the two
 * instances are independent.
 */
@DisplayName("LXMF outbound queue tracks instances, not hashes")
class QueuedInstanceTest : OutboundQueueTestBase() {

    /** Two instances of one message: the same hash, one for the direct send and one for the node. */
    private fun twoInstances(dest: Destination): Pair<LXMessage, LXMessage> {
        val timestamp = System.currentTimeMillis() / 1000.0
        val direct = messageTo(dest, resourceSizedContent, DeliveryMethod.DIRECT, timestamp)
        val propagated = messageTo(dest, resourceSizedContent, DeliveryMethod.PROPAGATED, timestamp)
        return direct to propagated
    }

    @Test
    fun `a direct send and a node upload of one message keep their own Resources`() = runBlocking {
        val dest = deliveryDestination(Identity.create())
        activePropagationLink()
        val (direct, propagated) = twoInstances(dest)
        val directResource = sendAsResource(direct, dest)

        router.handleOutbound(propagated)
        driveUntil { router.pendingResourceForTest(propagated) != null }

        assertContentEquals(direct.hash, propagated.hash, "the two instances share one hash")
        assertNotSame(directResource, router.pendingResourceForTest(propagated))
        assertSame(directResource, router.pendingResourceForTest(direct), "the upload did not replace the direct Resource")
        assertEquals(ResourceConstants.ADVERTISED, directResource.status, "the upload did not cancel the direct Resource")
        assertEquals(MessageState.SENDING, direct.state)
        assertEquals(MessageState.SENDING, propagated.state)
    }

    @Test
    fun `the direct send reports DELIVERED while its node upload is still in flight`() = runBlocking {
        val dest = deliveryDestination(Identity.create())
        activePropagationLink()
        val (direct, propagated) = twoInstances(dest)
        val directDeliveries = AtomicInteger()
        val propagatedDeliveries = AtomicInteger()
        direct.deliveryCallback = { directDeliveries.incrementAndGet() }
        propagated.deliveryCallback = { propagatedDeliveries.incrementAndGet() }
        val directResource = sendAsResource(direct, dest)
        router.handleOutbound(propagated)
        driveUntil { router.pendingResourceForTest(propagated) != null }
        val upload = assertNotNull(router.pendingResourceForTest(propagated))

        directResource.callbacks.completed?.invoke(directResource)

        assertEquals(MessageState.DELIVERED, direct.state)
        assertEquals(1, directDeliveries.get())
        assertNull(router.pendingResourceForTest(direct))
        assertEquals(MessageState.SENDING, propagated.state, "the upload is untouched")
        assertEquals(0, propagatedDeliveries.get())
        assertSame(upload, router.pendingResourceForTest(propagated))
        assertEquals(ResourceConstants.ADVERTISED, upload.status)
    }

    @Test
    fun `the direct send reports DELIVERED after its node upload is done`() = runBlocking {
        val dest = deliveryDestination(Identity.create())
        activePropagationLink()
        val (direct, propagated) = twoInstances(dest)
        val directDeliveries = AtomicInteger()
        val propagatedDeliveries = AtomicInteger()
        direct.deliveryCallback = { directDeliveries.incrementAndGet() }
        propagated.deliveryCallback = { propagatedDeliveries.incrementAndGet() }
        val directResource = sendAsResource(direct, dest)
        router.handleOutbound(propagated)
        driveUntil { router.pendingResourceForTest(propagated) != null }
        val upload = assertNotNull(router.pendingResourceForTest(propagated))

        upload.callbacks.completed?.invoke(upload)

        // The node has the copy: SENT is final for it, and the direct send goes on.
        assertEquals(MessageState.SENT, propagated.state)
        assertEquals(1, propagatedDeliveries.get())
        assertEquals(MessageState.SENDING, direct.state)
        assertSame(directResource, router.pendingResourceForTest(direct))

        directResource.callbacks.completed?.invoke(directResource)

        assertEquals(MessageState.DELIVERED, direct.state)
        assertEquals(1, directDeliveries.get())
        assertEquals(1, propagatedDeliveries.get())
    }

    @Test
    fun `a second instance of one hash starts its own pathless backoff`() = runBlocking {
        // No path to this destination, so every pass reschedules on the backoff curve. Both
        // instances go direct: a PROPAGATED one would fail at once for want of a node.
        val dest = deliveryDestination(Identity.create())
        val timestamp = System.currentTimeMillis() / 1000.0
        val first = messageTo(dest, "hello", timestamp = timestamp)
        val second = messageTo(dest, "hello", timestamp = timestamp)
        router.handleOutbound(first)
        driveUntil { first.nextDeliveryAttempt != null }

        // Two more pathless passes over the first instance: it is now three steps up the curve.
        repeat(2) {
            first.nextDeliveryAttempt = 0L
            driveUntil { first.nextDeliveryAttempt != 0L }
        }
        val firstWait = first.nextDeliveryAttempt!! - System.currentTimeMillis()
        assertTrue(firstWait > RetryBackoff.afterStep(1), "the first instance climbed the curve, waits $firstWait ms")

        router.handleOutbound(second)
        driveUntil { second.nextDeliveryAttempt != null }
        assertContentEquals(first.hash, second.hash, "the two instances share one hash")

        // Keyed by hash, the second instance would have inherited the first one's climb.
        val secondWait = second.nextDeliveryAttempt!! - System.currentTimeMillis()
        assertTrue(
            secondWait <= RetryBackoff.afterStep(0),
            "the second instance starts at the first step of the curve, waits $secondWait ms",
        )
    }
}
