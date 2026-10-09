package network.reticulum.lxmf

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import network.reticulum.identity.Identity
import network.reticulum.resource.ResourceConstants
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNotSame
import kotlin.test.assertNull
import kotlin.test.assertSame
import kotlin.test.assertTrue

/**
 * A node upload makes its proof-of-work stamp outside the processing lock, and its Resource is
 * wired up before it advertises.
 *
 * The stamp used to be made in `runBlocking` while `outboundProcessingMutex` was held, so every
 * other message waited the seconds a stamp takes on a phone. And the upload's Resource used to
 * advertise before its failed callback was set, so a failure in that gap left the message in
 * SENDING until the stall rule noticed, two minutes later.
 */
@DisplayName("LXMF propagation uploads make their stamp outside the processing lock")
class PropagationStampTest : OutboundQueueTestBase() {

    @Test
    fun `other messages are dispatched while a node upload's stamp is being made`() = runBlocking {
        activePropagationLink()
        val upload = messageTo(deliveryDestination(Identity.create()), "to the node", DeliveryMethod.PROPAGATED)
        val stampEntered = CompletableDeferred<Unit>()
        val stampRelease = CompletableDeferred<Unit>()
        router.testHookBeforePropagationStamp = {
            stampEntered.complete(Unit)
            stampRelease.await()
        }

        try {
            router.handleOutbound(upload)
            driveUntil { stampEntered.isCompleted }
            assertEquals(MessageState.SENDING, upload.state)
            assertNull(router.pendingResourceForTest(upload), "nothing is sent before the stamp is made")

            // With the stamp still being made, a pass over the queue runs and takes another
            // message. With the stamp made under the processing lock, no pass could run until
            // the stamp was done, and this timed out.
            val other = messageTo(deliveryDestination(Identity.create()), "meanwhile")
            router.handleOutbound(other)
            driveUntil { other.nextDeliveryAttempt != null }
        } finally {
            stampRelease.complete(Unit)
            router.testHookBeforePropagationStamp = null
        }

        driveUntil { router.pendingResourceForTest(upload) != null }
        val resource = assertNotNull(router.pendingResourceForTest(upload))
        assertEquals(ResourceConstants.ADVERTISED, resource.status)
        assertNotNull(resource.callbacks.failed, "the failed callback is in place on the advertised Resource")
        assertEquals(MessageState.SENDING, upload.state)
    }

    @Test
    fun `a failed upload is sent again`() = runBlocking {
        activePropagationLink()
        val upload = messageTo(deliveryDestination(Identity.create()), "to the node", DeliveryMethod.PROPAGATED)
        val failures = AtomicInteger()
        upload.failedCallback = { failures.incrementAndGet() }
        router.handleOutbound(upload)
        driveUntil { router.pendingResourceForTest(upload) != null }
        val resource = assertNotNull(router.pendingResourceForTest(upload))

        resource.callbacks.failed?.invoke(resource)
        driveUntil { upload.state == MessageState.OUTBOUND }

        assertNull(router.pendingResourceForTest(upload))
        assertEquals(1, failures.get())
        assertEquals(1, router.pendingOutboundCount(), "the upload stays queued for its retry")
    }

    @Test
    fun `a finished upload marks the instance SENT and it leaves the queue`() = runBlocking {
        activePropagationLink()
        val upload = messageTo(deliveryDestination(Identity.create()), "to the node", DeliveryMethod.PROPAGATED)
        val deliveries = AtomicInteger()
        upload.deliveryCallback = { deliveries.incrementAndGet() }
        router.handleOutbound(upload)
        driveUntil { router.pendingResourceForTest(upload) != null }
        val resource = assertNotNull(router.pendingResourceForTest(upload))

        resource.callbacks.completed?.invoke(resource)

        assertEquals(MessageState.SENT, upload.state)
        assertEquals(1, deliveries.get())
        assertNull(router.pendingResourceForTest(upload))

        driveUntil { runBlocking { router.pendingOutboundCount() } == 0 }
    }

    @Test
    fun `a replaced upload's failure does not reschedule the instance`() = runBlocking {
        activePropagationLink()
        val upload = messageTo(deliveryDestination(Identity.create()), "to the node", DeliveryMethod.PROPAGATED)
        val failures = AtomicInteger()
        upload.failedCallback = { failures.incrementAndGet() }
        router.handleOutbound(upload)
        driveUntil { router.pendingResourceForTest(upload) != null }
        val first = assertNotNull(router.pendingResourceForTest(upload))

        stall(upload)
        driveUntil { router.pendingResourceForTest(upload).let { it != null && it !== first } }
        val second = assertNotNull(router.pendingResourceForTest(upload))
        first.callbacks.failed?.invoke(first)
        delay(200)

        assertNotSame(first, second)
        assertEquals(ResourceConstants.FAILED, first.status, "the stall resend cancelled the first upload")
        assertEquals(MessageState.SENDING, upload.state)
        assertEquals(0, failures.get())
        assertSame(second, router.pendingResourceForTest(upload))
    }

    @Test
    fun `an upload cancelled while its stamp is being made is never advertised`() = runBlocking {
        val nodeLink = activePropagationLink()
        val upload = messageTo(deliveryDestination(Identity.create()), "to the node", DeliveryMethod.PROPAGATED)
        val stampEntered = CompletableDeferred<Unit>()
        val stampRelease = CompletableDeferred<Unit>()
        router.testHookBeforePropagationStamp = {
            stampEntered.complete(Unit)
            stampRelease.await()
        }

        try {
            router.handleOutbound(upload)
            driveUntil { stampEntered.isCompleted }

            assertTrue(router.cancelOutbound(upload))
        } finally {
            stampRelease.complete(Unit)
            router.testHookBeforePropagationStamp = null
        }
        delay(500)

        assertEquals(MessageState.CANCELLED, upload.state)
        assertNull(router.pendingResourceForTest(upload), "the stamped upload was dropped")
        assertTrue(nodeLink.readyForNewResource(), "nothing was advertised on the node link")
    }
}
