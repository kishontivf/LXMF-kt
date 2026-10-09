package network.reticulum.lxmf

import kotlinx.coroutines.delay
import kotlinx.coroutines.withTimeout
import network.reticulum.common.DestinationDirection
import network.reticulum.common.DestinationType
import network.reticulum.common.toHexString
import network.reticulum.destination.Destination
import network.reticulum.identity.Identity
import network.reticulum.link.Link
import network.reticulum.link.LinkConstants
import network.reticulum.resource.Resource
import network.reticulum.transport.Transport
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import kotlin.test.assertNotNull

/**
 * Shared setup for the tests that drive the outbound queue by hand.
 *
 * Nothing here starts [Transport], so nothing a link sends goes anywhere: a Resource is created
 * and advertised for real on a link that only looks established, which is all these tests read.
 * Each test gets a fresh router and closes it afterwards. The helpers follow [ResourceResendTest].
 */
abstract class OutboundQueueTestBase {

    protected lateinit var identity: Identity
    protected lateinit var router: LXMRouter

    /** Content too big for one link packet, so a message carrying it travels as a Resource. */
    protected val resourceSizedContent = "X".repeat(500)

    @BeforeEach
    fun setupRouter() {
        identity = Identity.create()
        router = LXMRouter(identity = identity)
    }

    @AfterEach
    fun teardownRouter() {
        router.close()
        Transport.pathTable.clear()
    }

    protected fun deliveryDestination(destIdentity: Identity): Destination =
        Destination.create(
            identity = destIdentity,
            direction = DestinationDirection.OUT,
            type = DestinationType.SINGLE,
            appName = "lxmf",
            aspects = arrayOf("delivery"),
        )

    /**
     * A message from this router's identity to [dest].
     *
     * With [timestamp] given, two calls build two instances with one hash, which is what a caller
     * does when it queues a message direct and then again for a propagation node.
     */
    protected fun messageTo(
        dest: Destination,
        content: String,
        desiredMethod: DeliveryMethod = DeliveryMethod.DIRECT,
        timestamp: Double? = null,
    ): LXMessage {
        val source = Destination.create(
            identity = identity,
            direction = DestinationDirection.IN,
            type = DestinationType.SINGLE,
            appName = "lxmf",
            aspects = arrayOf("delivery"),
        )
        val message = LXMessage.create(
            destination = dest,
            source = source,
            content = content,
            title = "t",
            desiredMethod = desiredMethod,
        )
        message.timestamp = timestamp
        return message
    }

    /**
     * A link to [dest] that looks established to the router: ACTIVE, active a moment ago so its
     * watchdog leaves it alone, and holding the key a real handshake would have derived, which
     * [Link.encrypt] needs when a Resource is built on it.
     */
    protected fun activeLinkTo(dest: Destination): Link {
        val link = Link.create(dest)
        setLinkField(link, "status", LinkConstants.ACTIVE)
        setLinkField(link, "activatedAt", System.currentTimeMillis())
        setLinkField(link, "derivedKey", ByteArray(LinkConstants.derivedKeyLength(LinkConstants.MODE_DEFAULT)) { it.toByte() })
        return link
    }

    /**
     * Gives the router an active propagation node with [stampCost] and an established link to it,
     * so a PROPAGATED message is uploaded on the next pass. Cost 1 keeps the proof of work to a
     * few hashes.
     */
    protected fun activePropagationLink(stampCost: Int = 1): Link {
        val nodeIdentity = Identity.create()
        val nodeDestHash = Destination.hash(nodeIdentity, "lxmf", "propagation")
        router.addPropagationNode(
            LXMRouter.PropagationNode(destHash = nodeDestHash, identity = nodeIdentity, stampCost = stampCost),
        )
        router.setActivePropagationNode(nodeDestHash.toHexString())

        val nodeDestination = Destination.create(
            identity = nodeIdentity,
            direction = DestinationDirection.OUT,
            type = DestinationType.SINGLE,
            appName = "lxmf",
            aspects = arrayOf("propagation"),
        )
        val link = activeLinkTo(nodeDestination)
        router.setPropagationLinkForTest(link)
        return link
    }

    /**
     * Queues [message] to a destination with an active direct link and drives the router until
     * a Resource is in flight for it, then returns that Resource.
     *
     * Driving is by hand: handleOutbound also launches a pass of its own, and whichever loses the
     * race for the processing mutex does nothing, so the loop polls until one of them got there.
     */
    protected suspend fun sendAsResource(message: LXMessage, dest: Destination): Resource {
        router.setDirectLinkForTest(dest.hash.toHexString(), activeLinkTo(dest))
        router.handleOutbound(message)
        driveUntil { router.pendingResourceForTest(message) != null }

        return assertNotNull(router.pendingResourceForTest(message))
    }

    /** Backdates the message's last send so the next pass sees a stalled transfer. */
    protected fun stall(message: LXMessage) {
        message.nextDeliveryAttempt = System.currentTimeMillis() - LXMRouter.SENDING_STALL_TIMEOUT - 1
    }

    protected suspend fun driveUntil(condition: () -> Boolean) {
        withTimeout(5_000) {
            while (!condition()) {
                router.processOutbound()
                delay(20)
            }
        }
    }

    /** A few passes over the queue, for asserting that they change nothing. */
    protected suspend fun driveAFewPasses() {
        repeat(3) {
            router.processOutbound()
            delay(20)
        }
    }

    /** These are what a handshake sets, and a handshake needs a peer; a test has none. */
    private fun setLinkField(link: Link, fieldName: String, value: Any?) {
        val field = Link::class.java.getDeclaredField(fieldName)
        field.isAccessible = true
        field.set(link, value)
    }
}
