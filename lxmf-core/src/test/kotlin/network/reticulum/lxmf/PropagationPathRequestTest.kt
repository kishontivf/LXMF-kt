package network.reticulum.lxmf

import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import network.reticulum.common.DestinationDirection
import network.reticulum.common.DestinationType
import network.reticulum.common.InterfaceMode
import network.reticulum.common.PacketType
import network.reticulum.common.RnsConstants
import network.reticulum.common.toHexString
import network.reticulum.common.toKey
import network.reticulum.destination.Destination
import network.reticulum.identity.Identity
import network.reticulum.packet.Packet
import network.reticulum.transport.InterfaceRef
import network.reticulum.transport.PathEntry
import network.reticulum.transport.PathState
import network.reticulum.transport.Transport
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

/**
 * A propagation node known from the saved list but without a path: a pull and a delivery ask for
 * the path first and link once it is there, as Python does. Before this, the link request went
 * out without a path, broadcast on every interface, and never reached a node behind a relay: a
 * Pixel's launch pull timed out after 60 s, every cycle.
 *
 * Drives the real [Transport] with a capturing interface, as [DirectDeliveryPathRequestTest]
 * does, and reads the path requests and link requests off it.
 */
@DisplayName("LXMRouter propagation path requests")
class PropagationPathRequestTest {

    private class CapturingInterface(
        override val name: String,
    ) : InterfaceRef {
        val sent = mutableListOf<ByteArray>()

        override val hash: ByteArray = ByteArray(RnsConstants.TRUNCATED_HASH_BYTES) { 0xAB.toByte() }
        override val canSend: Boolean = true
        override val canReceive: Boolean = true
        override val online: Boolean = true
        override val mode: InterfaceMode = InterfaceMode.FULL
        override val bitrate: Int = 1_000_000
        override val hwMtu: Int = RnsConstants.MTU

        override var tunnelId: ByteArray? = null
        override var wantsTunnel: Boolean = false

        override fun send(data: ByteArray) {
            synchronized(sent) { sent.add(data.copyOf()) }
        }

        fun snapshot(): List<ByteArray> = synchronized(sent) { sent.toList() }
    }

    private lateinit var iface: CapturingInterface
    private lateinit var router: LXMRouter
    private lateinit var nodeDestHash: ByteArray

    private val pathRequestDestHash: ByteArray =
        Destination.create(
            identity = null,
            direction = DestinationDirection.IN,
            type = DestinationType.PLAIN,
            appName = "rnstransport",
            aspects = arrayOf("path", "request"),
        ).hash

    @BeforeEach
    fun setup() {
        try {
            Transport.stop()
        } catch (_: Exception) {
            // Best-effort.
        }
        Transport.pathTable.clear()
        Transport.start(Identity.create(), enableTransport = false)
        iface = CapturingInterface(name = "capture-${System.nanoTime()}")
        Transport.registerInterface(iface)

        router = LXMRouter(identity = Identity.create())
        router.propagationPathWait = SHORT_PATH_WAIT

        val nodeIdentity = Identity.create()
        nodeDestHash = Destination.hash(nodeIdentity, "lxmf", "propagation")
        router.addPropagationNode(LXMRouter.PropagationNode(destHash = nodeDestHash, identity = nodeIdentity))
        router.setActivePropagationNode(nodeDestHash.toHexString())
    }

    @AfterEach
    fun teardown() {
        router.close()
        try {
            Transport.deregisterInterface(iface)
        } catch (_: Exception) {
            // Best-effort.
        }
        Transport.pathTable.clear()
        try {
            Transport.stop()
        } catch (_: Exception) {
            // Best-effort.
        }
    }

    @Test
    fun `requestMessagesFromPropagationNode should request a path and link once it arrives`() = runBlocking {
        // when
        router.requestMessagesFromPropagationNode()

        // then
        assertEquals(LXMRouter.PropagationTransferState.PATH_REQUESTED, router.propagationTransferState)
        assertEquals(1, pathRequestCount(), "one path request")
        assertEquals(0, linkRequestCount(), "no link request without a path")

        // when
        seedPath()

        // then
        withTimeout(WAIT_FOR_LINK) {
            while (linkRequestCount() == 0) delay(20)
        }
        assertEquals(1, linkRequestCount(), "one link request once the path is there")
        assertEquals(LXMRouter.PropagationTransferState.LINK_ESTABLISHING, router.propagationTransferState)
    }

    @Test
    fun `requestMessagesFromPropagationNode should end NO_PATH when no path comes`() = runBlocking {
        // when
        router.requestMessagesFromPropagationNode()

        // then
        withTimeout(WAIT_FOR_LINK) {
            while (router.propagationTransferState != LXMRouter.PropagationTransferState.NO_PATH) delay(20)
        }
        assertEquals(1, pathRequestCount(), "one path request")
        assertEquals(0, linkRequestCount(), "no link request without a path")
    }

    @Test
    fun `requestMessagesFromPropagationNode should link at once when a path is known`() = runBlocking {
        // given
        seedPath()

        // when
        router.requestMessagesFromPropagationNode()

        // then
        assertEquals(LXMRouter.PropagationTransferState.LINK_ESTABLISHING, router.propagationTransferState)
        assertEquals(0, pathRequestCount(), "no path request when a path is known")
        assertEquals(1, linkRequestCount(), "one link request")
    }

    @Test
    fun `processOutbound should request a path for a propagated message and link once it arrives`() = runBlocking {
        // given
        val message = propagatedMessage()
        router.handleOutbound(message)

        // when
        withTimeout(WAIT_FOR_LINK) {
            while (pathRequestCount() == 0) {
                router.processOutbound()
                delay(20)
            }
        }
        repeat(3) {
            router.processOutbound()
            delay(20)
        }

        // then
        assertEquals(1, pathRequestCount(), "one path request, and no second one before the wait is up")
        assertEquals(0, linkRequestCount(), "no link request without a path")
        assertEquals(0, message.deliveryAttempts, "a pathless pass bills no attempt")

        // when
        seedPath()
        message.nextDeliveryAttempt = 0L
        withTimeout(WAIT_FOR_LINK) {
            while (linkRequestCount() == 0) {
                router.processOutbound()
                delay(20)
            }
        }

        // then
        assertEquals(1, linkRequestCount(), "one link request once the path is there")
        assertEquals(1, pathRequestCount(), "no further path request")
    }

    @Test
    fun `processOutbound should link at once for a propagated message when a path is known`() = runBlocking {
        // given
        seedPath()
        val message = propagatedMessage()
        router.handleOutbound(message)

        // when
        withTimeout(WAIT_FOR_LINK) {
            while (linkRequestCount() == 0) {
                router.processOutbound()
                delay(20)
            }
        }

        // then
        assertEquals(0, pathRequestCount(), "no path request when a path is known")
        assertEquals(1, linkRequestCount(), "one link request")
    }

    /** A PROPAGATED message from this router to some peer. The node, not the peer, is what the link goes to. */
    private fun propagatedMessage(): LXMessage {
        val source = Destination.create(
            identity = Identity.create(),
            direction = DestinationDirection.IN,
            type = DestinationType.SINGLE,
            appName = "lxmf",
            aspects = arrayOf("delivery"),
        )
        val peer = Destination.create(
            identity = Identity.create(),
            direction = DestinationDirection.OUT,
            type = DestinationType.SINGLE,
            appName = "lxmf",
            aspects = arrayOf("delivery"),
        )

        return LXMessage.create(
            destination = peer,
            source = source,
            content = "to the node",
            title = "t",
            desiredMethod = DeliveryMethod.PROPAGATED,
        )
    }

    /** A live, direct path to the node over the capturing interface, as an announce would leave it. */
    private fun seedPath() {
        val now = System.currentTimeMillis()

        Transport.pathTable[nodeDestHash.toKey()] = PathEntry(
            timestamp = now,
            nextHop = ByteArray(RnsConstants.TRUNCATED_HASH_BYTES) { 0xDD.toByte() },
            hops = 1,
            expires = now + 3_600_000L,
            randomBlobs = mutableListOf(),
            receivingInterfaceHash = iface.hash,
            announcePacketHash = ByteArray(RnsConstants.TRUNCATED_HASH_BYTES) { 0xCC.toByte() },
            state = PathState.ACTIVE,
            failureCount = 0,
        )
    }

    /**
     * Distinct path requests for the node, by request tag: one `Transport.requestPath` call, however
     * many times its packet went out. As [DirectDeliveryPathRequestTest] counts them.
     */
    private fun pathRequestCount(): Int =
        iface.snapshot().mapNotNull { wire ->
            val packet = Packet.unpack(wire) ?: return@mapNotNull null
            val isForNode = packet.destinationHash.contentEquals(pathRequestDestHash) &&
                packet.data.size >= 2 * RnsConstants.TRUNCATED_HASH_BYTES &&
                packet.data.copyOfRange(0, RnsConstants.TRUNCATED_HASH_BYTES).contentEquals(nodeDestHash)

            if (!isForNode) {
                null
            } else {
                packet.data.copyOfRange(packet.data.size - RnsConstants.TRUNCATED_HASH_BYTES, packet.data.size).toHexString()
            }
        }.toSet().size

    private fun linkRequestCount(): Int =
        iface.snapshot().count { wire -> Packet.unpack(wire)?.packetType == PacketType.LINKREQUEST }

    private companion object {

        /** Short enough for a test that waits the whole of it: the real wait is fifteen seconds. */
        const val SHORT_PATH_WAIT = 300L

        /** Real time, so generous: a link request follows a path within a poll or two. */
        const val WAIT_FOR_LINK = 5_000L
    }
}
