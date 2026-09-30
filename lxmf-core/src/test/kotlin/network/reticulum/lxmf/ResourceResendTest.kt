package network.reticulum.lxmf

import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import network.reticulum.common.DestinationDirection
import network.reticulum.common.DestinationType
import network.reticulum.common.RnsConstants
import network.reticulum.common.toHexString
import network.reticulum.common.toKey
import network.reticulum.destination.Destination
import network.reticulum.identity.Identity
import network.reticulum.link.Link
import network.reticulum.link.LinkConstants
import network.reticulum.packet.Packet
import network.reticulum.resource.Resource
import network.reticulum.resource.ResourceConstants
import network.reticulum.transport.PathEntry
import network.reticulum.transport.PathState
import network.reticulum.transport.Transport
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertSame
import kotlin.test.assertTrue

/**
 * Regression guard for the resend storm in `resource-resend-plan.md`.
 *
 * A Pixel 5 ran out of threads after five hours of resending one Resource to an iPhone: the
 * stall rule in [LXMRouter.processOutbound] fired on every pass, each resend left the Resource it
 * replaced alive, nothing counted the resends against the delivery budget, and the Resource went
 * over a link the iPhone had opened without identifying, on which it ignores Resources. These
 * tests pin the rules that stop that: a send cancels the Resource it replaces, a replaced
 * Resource's callbacks change nothing, a stall resend happens once per stall and is billed as an
 * attempt, and — as in Python — a link is a backchannel only once its remote has identified on it.
 *
 * The link is a real [Link] from [Link.create], set ACTIVE by hand and given a key so it can
 * encrypt. [Transport] is not started, so nothing a link sends goes anywhere — the Resource is
 * created and advertised for real, and that is all these rules read.
 */
@DisplayName("LXMF Resource resends and backchannels")
class ResourceResendTest {

    private lateinit var identity: Identity
    private lateinit var router: LXMRouter

    @BeforeEach
    fun setup() {
        identity = Identity.create()
        router = LXMRouter(identity = identity)
    }

    @AfterEach
    fun teardown() {
        router.close()
        Transport.pathTable.clear()
    }

    private fun deliveryDestination(destIdentity: Identity): Destination =
        Destination.create(
            identity = destIdentity,
            direction = DestinationDirection.OUT,
            type = DestinationType.SINGLE,
            appName = "lxmf",
            aspects = arrayOf("delivery"),
        )

    /** A message too big for one link packet, so it travels as a Resource. */
    private fun resourceMessageTo(dest: Destination): LXMessage {
        val source = Destination.create(
            identity = identity,
            direction = DestinationDirection.IN,
            type = DestinationType.SINGLE,
            appName = "lxmf",
            aspects = arrayOf("delivery"),
        )
        return LXMessage.create(
            destination = dest,
            source = source,
            content = "X".repeat(500),
            title = "resource",
            desiredMethod = DeliveryMethod.DIRECT,
        )
    }

    /**
     * A packed message from [sender] to this router's delivery destination, with the sender's
     * identity remembered so the router can validate its signature, as [LXMRouterTest] does.
     */
    private fun packedMessageFrom(sender: Identity, body: String): ByteArray {
        val senderDelivery = Destination.create(
            identity = sender,
            direction = DestinationDirection.IN,
            type = DestinationType.SINGLE,
            appName = "lxmf",
            aspects = arrayOf("delivery"),
        )
        Identity.remember(
            packetHash = ByteArray(32) { it.toByte() },
            destHash = senderDelivery.hash,
            publicKey = sender.getPublicKey(),
        )
        return LXMessage.create(
            destination = deliveryDestination(identity),
            source = senderDelivery,
            content = body,
            title = "hello",
        ).pack()
    }

    /**
     * A link to [dest] that looks established to the router: ACTIVE, active a moment ago so its
     * watchdog leaves it alone, and holding the key a real handshake would have derived, which
     * [Link.encrypt] needs when a Resource is built on it.
     */
    private fun activeLinkTo(dest: Destination): Link {
        val link = Link.create(dest)
        setLinkField(link, "status", LinkConstants.ACTIVE)
        setLinkField(link, "activatedAt", System.currentTimeMillis())
        setLinkField(link, "derivedKey", ByteArray(LinkConstants.derivedKeyLength(LinkConstants.MODE_DEFAULT)) { it.toByte() })
        return link
    }

    /** These are what a handshake sets, and a handshake needs a peer; a test has none. */
    private fun setLinkField(link: Link, fieldName: String, value: Any?) {
        val field = Link::class.java.getDeclaredField(fieldName)
        field.isAccessible = true
        field.set(link, value)
    }

    /**
     * Hands the router a DIRECT message that arrived over [link], the way a concluded inbound
     * Resource does. Reflection, as in [LXMRouterTest]: a full inbound transfer needs a peer.
     */
    private fun deliverOverLink(packed: ByteArray, link: Link) {
        val processInbound = LXMRouter::class.java.getDeclaredMethod(
            "processInboundDelivery",
            ByteArray::class.java,
            DeliveryMethod::class.java,
            Destination::class.java,
            Link::class.java,
            Packet::class.java,
        )
        processInbound.isAccessible = true
        processInbound.invoke(router, packed, DeliveryMethod.DIRECT, null, link, null)
    }

    /**
     * Plays out a remote opening [link] to [ours] and then identifying on it as [remote]: the
     * router's link-established handling, then the callback the link fires once the remote's
     * identification has been verified.
     */
    private fun remoteIdentifiesOn(link: Link, ours: Destination, remote: Identity) {
        val established = LXMRouter::class.java.getDeclaredMethod(
            "handleDeliveryLinkEstablished",
            Link::class.java,
            Destination::class.java,
        )
        established.isAccessible = true
        established.invoke(router, link, ours)

        link.callbacks.remoteIdentified?.invoke(link, remote)
    }

    /** A usable path, so the router may open a link. Nothing here is transmitted. */
    private fun livePathEntry(): PathEntry {
        val now = System.currentTimeMillis()
        return PathEntry(
            timestamp = now,
            nextHop = ByteArray(RnsConstants.TRUNCATED_HASH_BYTES) { 0xDD.toByte() },
            hops = 1,
            expires = now + 3_600_000L,
            randomBlobs = mutableListOf(),
            receivingInterfaceHash = ByteArray(RnsConstants.TRUNCATED_HASH_BYTES) { 0xAA.toByte() },
            announcePacketHash = ByteArray(RnsConstants.TRUNCATED_HASH_BYTES) { 0xCC.toByte() },
            state = PathState.ACTIVE,
            failureCount = 0,
        )
    }

    /**
     * Queues [message] to a destination with an active direct link and drives the router until
     * a Resource is in flight for it, then returns the message's hash and that Resource.
     *
     * Driving is by hand: handleOutbound also launches a pass of its own, and whichever loses the
     * race for the processing mutex does nothing, so the loop polls until one of them got there.
     * It waits for the Resource rather than for SENDING, which sendViaLink sets before the
     * Resource exists.
     */
    private suspend fun sendAsResource(message: LXMessage, dest: Destination): Pair<String, Resource> {
        router.setDirectLinkForTest(dest.hash.toHexString(), activeLinkTo(dest))
        router.handleOutbound(message)

        val hashHex = message.hash!!.toHexString()
        driveUntil { router.pendingResourceForTest(hashHex) != null }

        return hashHex to assertNotNull(router.pendingResourceForTest(hashHex))
    }

    /** Backdates the message's last send so the next pass sees a stalled transfer. */
    private fun stall(message: LXMessage) {
        message.nextDeliveryAttempt = System.currentTimeMillis() - LXMRouter.SENDING_STALL_TIMEOUT - 1
    }

    private suspend fun driveUntil(condition: () -> Boolean) {
        withTimeout(5_000) {
            while (!condition()) {
                router.processOutbound()
                delay(20)
            }
        }
    }

    @Test
    fun `a resend cancels the message's previous Resource`() = runBlocking {
        val dest = deliveryDestination(Identity.create())
        val message = resourceMessageTo(dest)
        val (hashHex, first) = sendAsResource(message, dest)
        assertEquals(ResourceConstants.ADVERTISED, first.status)

        stall(message)
        driveUntil { router.pendingResourceForTest(hashHex) !== first }

        val second = assertNotNull(router.pendingResourceForTest(hashHex))
        assertEquals(ResourceConstants.FAILED, first.status, "the replaced Resource is cancelled")
        // Cancelling freed the link, so the new Resource advertised at once rather than parking
        // in QUEUED behind the old one with a polling thread of its own.
        assertEquals(ResourceConstants.ADVERTISED, second.status)
        assertEquals(MessageState.SENDING, message.state)
    }

    @Test
    fun `a replaced Resource's failure does not reschedule the message and its completion does not deliver it`() = runBlocking {
        val dest = deliveryDestination(Identity.create())
        val message = resourceMessageTo(dest)
        val failures = AtomicInteger()
        val deliveries = AtomicInteger()
        message.failedCallback = { failures.incrementAndGet() }
        message.deliveryCallback = { deliveries.incrementAndGet() }
        val (hashHex, first) = sendAsResource(message, dest)

        stall(message)
        driveUntil { router.pendingResourceForTest(hashHex) !== first }
        val second = router.pendingResourceForTest(hashHex)
        val sentAt = message.nextDeliveryAttempt

        // Cancelling `first` fired its failed callback during the resend. That used to put the
        // message back on the retry curve; now the callback finds the Resource is no longer live.
        assertEquals(0, failures.get(), "a replaced Resource's failure must not reschedule")
        assertEquals(MessageState.SENDING, message.state)

        // Nor does anything it reports later.
        first.callbacks.failed?.invoke(first)
        first.callbacks.completed?.invoke(first)

        assertEquals(0, failures.get())
        assertEquals(0, deliveries.get(), "a replaced Resource's completion must not deliver")
        assertEquals(MessageState.SENDING, message.state)
        assertEquals(sentAt, message.nextDeliveryAttempt)
        assertSame(second, router.pendingResourceForTest(hashHex))
    }

    @Test
    fun `the stall rule resends once per SENDING_STALL_TIMEOUT, not on every pass`() = runBlocking {
        val dest = deliveryDestination(Identity.create())
        val message = resourceMessageTo(dest)
        val (hashHex, first) = sendAsResource(message, dest)

        // The first send sets the clock the stall rule reads. It used to leave it unset, so the
        // rule fired on the first pass after a send and then on every pass after that.
        val sentAt = assertNotNull(message.nextDeliveryAttempt, "a Resource send records when it was made")
        assertTrue(System.currentTimeMillis() - sentAt < 5_000, "the send time is now, not a past failure's")

        stall(message)
        driveUntil { router.pendingResourceForTest(hashHex) !== first }
        val second = router.pendingResourceForTest(hashHex)
        assertEquals(1, message.deliveryAttempts)

        // Further passes see a send made moments ago and leave it alone.
        repeat(3) {
            router.processOutbound()
            delay(20)
        }

        assertSame(second, router.pendingResourceForTest(hashHex))
        assertEquals(1, message.deliveryAttempts)
        assertEquals(MessageState.SENDING, message.state)
    }

    @Test
    fun `stall resends count toward MAX_DELIVERY_ATTEMPTS and the message parks after eight`() = runBlocking {
        val dest = deliveryDestination(Identity.create())
        val message = resourceMessageTo(dest)
        val failures = AtomicInteger()
        message.failedCallback = { failures.incrementAndGet() }
        val (hashHex, _) = sendAsResource(message, dest)

        repeat(LXMRouter.MAX_DELIVERY_ATTEMPTS) {
            val current = router.pendingResourceForTest(hashHex)
            stall(message)
            driveUntil { router.pendingResourceForTest(hashHex) !== current }
        }

        // Eight unanswered sends: the route is written off and the message parked, as it is for
        // a link that never answers — not sent a ninth time. Its last Resource is cancelled too.
        assertNull(router.pendingResourceForTest(hashHex), "the stalled Resource is cancelled on parking")
        assertEquals(MessageState.OUTBOUND, message.state)
        assertEquals(0, message.deliveryAttempts, "parking hands the attempts back")
        assertEquals(0, failures.get(), "only MAX_OUTBOUND_AGE may fail a message")
        val wait = message.nextDeliveryAttempt!! - System.currentTimeMillis()
        assertTrue(
            wait > 0 && wait <= LXMRouter.UNPROVEN_ROUTE_RETRY_WAIT,
            "parked for at most UNPROVEN_ROUTE_RETRY_WAIT, was $wait ms",
        )

        // And it stays parked: no pass sends it again.
        repeat(3) {
            router.processOutbound()
            delay(20)
        }

        assertNull(router.pendingResourceForTest(hashHex))
        assertEquals(MessageState.OUTBOUND, message.state)
    }

    @Test
    fun `a DIRECT message over a link whose remote has not identified does not make it a backchannel`() = runBlocking {
        router.registerDeliveryIdentity(identity, "me")
        val sender = Identity.create()
        val senderDelivery = deliveryDestination(sender)
        val senderHex = senderDelivery.hash.toHexString()
        val theirLink = activeLinkTo(senderDelivery)
        deliverOverLink(packedMessageFrom(sender, "hello"), theirLink)
        Transport.pathTable[senderDelivery.hash.toKey()] = livePathEntry()
        val reply = resourceMessageTo(senderDelivery)

        router.handleOutbound(reply)
        driveUntil { router.directLinkForTest(senderHex) != null || reply.state == MessageState.SENDING }

        // As in Python, only a remote that identifies on its link earns a backchannel. The reply
        // opens a link of its own and puts nothing on the unidentified one — which, if it is an
        // iPhone's, would ignore a Resource without a word.
        assertNotNull(router.directLinkForTest(senderHex), "the reply opens a link of its own")
        assertTrue(theirLink.readyForNewResource(), "nothing is advertised on the unidentified link")
        assertNull(router.pendingResourceForTest(reply.hash!!.toHexString()))
        assertEquals(MessageState.OUTBOUND, reply.state)
    }

    @Test
    fun `a link whose remote identified is a backchannel, and a Resource-sized reply may use it`() = runBlocking {
        val ours = router.registerDeliveryIdentity(identity, "me")
        val sender = Identity.create()
        val senderDelivery = deliveryDestination(sender)
        val senderHex = senderDelivery.hash.toHexString()
        val theirLink = activeLinkTo(senderDelivery)
        remoteIdentifiesOn(theirLink, ours, sender)
        val reply = resourceMessageTo(senderDelivery)

        router.handleOutbound(reply)
        val replyHex = reply.hash!!.toHexString()
        driveUntil { router.pendingResourceForTest(replyHex) != null || router.directLinkForTest(senderHex) != null }

        // Python's delivery_remote_identified: the identified link is the backchannel, and the
        // DIRECT branch takes it for any representation before opening a link of its own.
        assertNotNull(router.pendingResourceForTest(replyHex), "the reply goes as a Resource over the backchannel")
        assertFalse(theirLink.readyForNewResource(), "the Resource is advertised on the identified link")
        assertNull(router.directLinkForTest(senderHex), "no link of its own is opened")
        assertEquals(MessageState.SENDING, reply.state)
    }
}
