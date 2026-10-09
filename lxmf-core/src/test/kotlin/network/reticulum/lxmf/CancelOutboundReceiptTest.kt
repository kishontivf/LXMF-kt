package network.reticulum.lxmf

import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import network.reticulum.common.InterfaceMode
import network.reticulum.common.RnsConstants
import network.reticulum.common.toKey
import network.reticulum.destination.Destination
import network.reticulum.identity.Identity
import network.reticulum.packet.PacketReceipt
import network.reticulum.transport.InterfaceRef
import network.reticulum.transport.PathEntry
import network.reticulum.transport.PathState
import network.reticulum.transport.Transport
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

/**
 * What a packet already in flight may still do to a cancelled instance.
 *
 * A delivery proof still calls the instance's `deliveryCallback`, because the peer did receive the
 * message: that is the one callback allowed after a cancel. A receipt that times out puts nothing
 * back. Drives the real [Transport] with a capturing interface, as [DirectDeliveryPathRequestTest]
 * does, because a receipt only exists for a packet the transport actually sent.
 */
@DisplayName("LXMRouter.cancelOutbound and packets already in flight")
class CancelOutboundReceiptTest : OutboundQueueTestBase() {

    private class CapturingInterface(
        override val name: String,
    ) : InterfaceRef {
        override val hash: ByteArray = ByteArray(RnsConstants.TRUNCATED_HASH_BYTES) { 0xAA.toByte() }
        override val canSend: Boolean = true
        override val canReceive: Boolean = true
        override val online: Boolean = true
        override val mode: InterfaceMode = InterfaceMode.FULL
        override val bitrate: Int = 1_000_000
        override val hwMtu: Int = RnsConstants.MTU

        override var tunnelId: ByteArray? = null
        override var wantsTunnel: Boolean = false

        override fun send(data: ByteArray) = Unit
    }

    private lateinit var iface: CapturingInterface

    @BeforeEach
    fun startTransport() {
        try {
            Transport.stop()
        } catch (_: Exception) {
            // Best-effort.
        }
        Transport.pathTable.clear()
        Transport.start(Identity.create(), enableTransport = false)
        iface = CapturingInterface(name = "capture-${System.nanoTime()}")
        Transport.registerInterface(iface)
    }

    @AfterEach
    fun stopTransport() {
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

    private fun livePathEntry(): PathEntry {
        val now = System.currentTimeMillis()
        return PathEntry(
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
     * Sends [message] opportunistically over the capturing interface and returns the receipt the
     * router registered its callbacks on. Transport's receipt list is private; the newest entry
     * is the one send this test made.
     */
    private suspend fun sendOpportunistically(message: LXMessage, dest: Destination): PacketReceipt {
        Transport.pathTable[dest.hash.toKey()] = livePathEntry()
        router.handleOutbound(message)
        driveUntil { message.state == MessageState.SENT }

        val field = Transport.javaClass.getDeclaredField("receipts")
        field.isAccessible = true
        @Suppress("UNCHECKED_CAST")
        val receipts = field.get(Transport) as List<PacketReceipt>

        return assertNotNull(receipts.lastOrNull(), "the send left a receipt")
    }

    @Test
    fun `a proof for a packet in flight still delivers a cancelled instance`() = runBlocking {
        val dest = deliveryDestination(Identity.create())
        val message = messageTo(dest, "hello", DeliveryMethod.OPPORTUNISTIC)
        val deliveries = AtomicInteger()
        message.deliveryCallback = { deliveries.incrementAndGet() }
        val receipt = sendOpportunistically(message, dest)
        assertTrue(router.cancelOutbound(message))
        assertEquals(MessageState.CANCELLED, message.state)

        receipt.callbacks.delivery?.invoke(receipt)

        assertEquals(MessageState.DELIVERED, message.state, "a late proof counts as delivered")
        assertEquals(1, deliveries.get())
    }

    @Test
    fun `a receipt that times out does not put a cancelled instance back`() = runBlocking {
        val dest = deliveryDestination(Identity.create())
        val message = messageTo(dest, "hello", DeliveryMethod.OPPORTUNISTIC)
        val failures = AtomicInteger()
        message.failedCallback = { failures.incrementAndGet() }
        val receipt = sendOpportunistically(message, dest)
        assertTrue(router.cancelOutbound(message))

        receipt.callbacks.timeout?.invoke(receipt)
        delay(200)

        assertEquals(MessageState.CANCELLED, message.state)
        assertEquals(0, router.pendingOutboundCount())
        assertEquals(0, failures.get())
    }

    @Test
    fun `a receipt that times out still puts a queued instance back`() = runBlocking {
        val dest = deliveryDestination(Identity.create())
        val message = messageTo(dest, "hello", DeliveryMethod.OPPORTUNISTIC)
        val failures = AtomicInteger()
        message.failedCallback = { failures.incrementAndGet() }
        val receipt = sendOpportunistically(message, dest)

        receipt.callbacks.timeout?.invoke(receipt)
        driveUntil { message.state == MessageState.OUTBOUND }

        assertEquals(1, failures.get())
        assertEquals(1, router.pendingOutboundCount())
    }
}
