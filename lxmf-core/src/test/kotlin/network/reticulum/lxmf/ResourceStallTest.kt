package network.reticulum.lxmf

import kotlinx.coroutines.runBlocking
import network.reticulum.common.toHexString
import network.reticulum.crypto.Hashes
import network.reticulum.identity.Identity
import network.reticulum.resource.Resource
import network.reticulum.resource.ResourceConstants
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import kotlin.random.Random
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertSame
import kotlin.test.assertTrue

/**
 * Pins the stall rule in [LXMRouter.processOutbound] to inactivity rather than age.
 *
 * On 2026-10-13 two photos went from a Pixel 8 Pro to a Pixel 10 Pro over Bluetooth alone. At
 * about a kilobyte a second, each needed about two minutes. The rule then measured a transfer's
 * age from its send. It cancelled each photo at [LXMRouter.SENDING_STALL_TIMEOUT] while it was
 * still moving. The second photo's wait behind the first counted against it too. Six restarts
 * later both arrived, only because one of them got enough time alone.
 *
 * These tests pin the corrected rule. A transfer that keeps showing signs of life is left alone,
 * however long it takes. A Resource waiting its turn on the link is not on the clock. One that
 * shows nothing for the timeout is sent again and billed an attempt. A sign of life is the
 * Resource's own last activity: its advertisement, or a part going out. The peer's part request
 * is played by hand, because nothing here has a peer.
 */
@DisplayName("LXMF Resource stall rule measures inactivity")
class ResourceStallTest : OutboundQueueTestBase() {

    /**
     * Content that bz2 cannot squeeze into one part: four thousand hex digits of seeded random
     * bytes. [resourceSizedContent] is a run of one letter, which compresses to a single part.
     */
    private val multiPartContent = ByteArray(2_000).also { Random(7).nextBytes(it) }.toHexString()

    @Test
    fun `a transfer that keeps making progress is not sent again however long it has been running`() = runBlocking {
        val dest = deliveryDestination(Identity.create())
        val message = messageTo(dest, multiPartContent)
        val resource = sendAsResource(message, dest)
        assertTrue(resource.parts.size >= 2, "the content must take more than one part")
        assertEquals(ResourceConstants.ADVERTISED, resource.status)

        // By the old measure, the send is long past the timeout.
        message.nextDeliveryAttempt = System.currentTimeMillis() - 2 * LXMRouter.SENDING_STALL_TIMEOUT

        // The peer's first part request comes late in the timeout, and a part goes out.
        router.ageTransferForTest(message, LATE_IN_THE_TIMEOUT)
        requestPart(resource, 0)
        driveAFewPasses()

        assertSame(resource, router.pendingResourceForTest(message), "a part going out is a sign of life")
        assertEquals(ResourceConstants.TRANSFERRING, resource.status)

        // The next request comes late in the timeout again. The transfer is now older than the
        // timeout, and still moving.
        router.ageTransferForTest(message, LATE_IN_THE_TIMEOUT)
        requestPart(resource, 1)
        driveAFewPasses()

        assertSame(resource, router.pendingResourceForTest(message))
        assertEquals(2, resource.sentParts)
        assertEquals(0, message.deliveryAttempts)
        assertEquals(MessageState.SENDING, message.state)
    }

    @Test
    fun `a Resource waiting its turn behind another transfer on the link is not on the clock`() = runBlocking {
        val dest = deliveryDestination(Identity.create())
        val first = messageTo(dest, resourceSizedContent)
        val second = messageTo(dest, resourceSizedContent)
        router.setDirectLinkForTest(dest.hash.toHexString(), activeLinkTo(dest))
        router.handleOutbound(first)
        driveUntil { router.pendingResourceForTest(first) != null }
        router.handleOutbound(second)
        driveUntil { router.pendingResourceForTest(second) != null }
        val waiting = assertNotNull(router.pendingResourceForTest(second))
        assertEquals(ResourceConstants.QUEUED, waiting.status, "the link is busy with the first transfer")

        // Longer than the timeout in the queue, and nothing happens to it.
        router.ageTransferForTest(second, LXMRouter.SENDING_STALL_TIMEOUT + 1)
        driveAFewPasses()

        assertSame(waiting, router.pendingResourceForTest(second))
        assertEquals(0, second.deliveryAttempts)

        // The first transfer ends and the link takes the second. Its clock starts here, so the
        // wait does not count.
        assertTrue(router.cancelOutbound(first))
        driveUntil { waiting.status == ResourceConstants.ADVERTISED }
        driveAFewPasses()

        assertSame(waiting, router.pendingResourceForTest(second), "the wait did not count")
        assertEquals(0, second.deliveryAttempts)

        // From here it is on the clock like any other transfer.
        router.ageTransferForTest(second, LXMRouter.SENDING_STALL_TIMEOUT + 1)
        driveUntil { router.pendingResourceForTest(second) !== waiting }

        assertEquals(ResourceConstants.FAILED, waiting.status, "the stalled Resource is cancelled")
        assertEquals(1, second.deliveryAttempts)
    }

    @Test
    fun `a transfer with no sign of life for SENDING_STALL_TIMEOUT is sent again and billed an attempt`() = runBlocking {
        val dest = deliveryDestination(Identity.create())
        val message = messageTo(dest, multiPartContent)
        val resource = sendAsResource(message, dest)
        requestPart(resource, 0)
        driveAFewPasses()
        assertSame(resource, router.pendingResourceForTest(message), "it got going")

        // Then nothing, for the whole timeout.
        router.ageTransferForTest(message, LXMRouter.SENDING_STALL_TIMEOUT + 1)
        driveUntil { router.pendingResourceForTest(message) !== resource }

        assertEquals(ResourceConstants.FAILED, resource.status, "the stalled Resource is cancelled")
        assertNotNull(router.pendingResourceForTest(message), "a new Resource is in flight")
        assertEquals(1, message.deliveryAttempts)
        assertEquals(MessageState.SENDING, message.state)
    }

    /**
     * Plays the peer asking for part [index] of [resource], which is what makes a sender send a
     * part. The request is the one `Resource.handleRequest` parses: a flag byte, the resource
     * hash, then the map hash of each wanted part.
     */
    private fun requestPart(resource: Resource, index: Int) {
        val part = assertNotNull(resource.parts[index])
        val mapHash = Hashes.fullHash(part + resource.randomHash).copyOf(ResourceConstants.MAPHASH_LEN)

        resource.handleRequest(byteArrayOf(ResourceConstants.HASHMAP_IS_NOT_EXHAUSTED.toByte()) + resource.hash + mapHash)
    }

    private companion object {
        /** Most of the timeout, so that two of these in a row take a transfer past it. */
        const val LATE_IN_THE_TIMEOUT = LXMRouter.SENDING_STALL_TIMEOUT - 10_000L
    }
}
