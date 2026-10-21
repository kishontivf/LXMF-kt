package network.reticulum.lxmf

import network.reticulum.common.DestinationDirection
import network.reticulum.common.DestinationType
import network.reticulum.destination.Destination
import network.reticulum.identity.Identity
import network.reticulum.link.Link
import network.reticulum.packet.Packet
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.concurrent.CopyOnWriteArrayList

/**
 * The delivery policy, and the proof that now follows admission.
 *
 * Messages come in through the entry points Reticulum uses: the delivery destination's packet
 * callback for an opportunistic packet, and a delivery link's packet callback for a direct one.
 * Nothing leaves [network.reticulum.transport.Transport] in a unit test, so the proofs are read
 * from [LXMRouter.proveForTest].
 */
class DeliveryPolicyTest {

    private lateinit var router: LXMRouter
    private lateinit var ownIdentity: Identity
    private lateinit var deliveryDestination: Destination
    private lateinit var senderIdentity: Identity
    private lateinit var senderDeliveryHash: ByteArray
    private val proven = CopyOnWriteArrayList<Packet>()
    private val delivered = CopyOnWriteArrayList<LXMessage>()
    private val events = CopyOnWriteArrayList<String>()

    @BeforeEach
    fun setup() {
        ownIdentity = Identity.create()
        router = LXMRouter(identity = ownIdentity)
        deliveryDestination = router.registerDeliveryIdentity(ownIdentity, "Receiver")
        senderIdentity = Identity.create()
        senderDeliveryHash = Destination.hash(senderIdentity, "lxmf", "delivery")

        // The router verifies a signature only against an identity it can recall.
        Identity.remember(
            packetHash = ByteArray(32) { it.toByte() },
            destHash = senderDeliveryHash,
            publicKey = senderIdentity.getPublicKey(),
        )

        router.proveForTest = { packet ->
            proven.add(packet)
            events.add(PROOF)
        }
        router.registerDeliveryCallback { message ->
            delivered.add(message)
            events.add(DELIVERED)
        }
    }

    @AfterEach
    fun teardown() {
        router.close()
    }

    @Test
    fun `deliveryPolicy should withhold the proof and the delivery from a refused message`() {
        // given
        router.deliveryPolicy = DeliveryPolicy { _, _ -> false }

        // when
        deliverOpportunistically(packedMessage("refused"))

        // then
        assertTrue(delivered.isEmpty(), "a refused message must not reach the host")
        assertTrue(proven.isEmpty(), "a refused message must not be proven")
    }

    @Test
    fun `deliveryPolicy should withhold the proof and the delivery from a refused message on a link`() {
        // given
        router.deliveryPolicy = DeliveryPolicy { _, _ -> false }

        // when
        deliverOverLink(packedMessage("refused on a link"))

        // then
        assertTrue(delivered.isEmpty(), "a refused message must not reach the host")
        assertTrue(proven.isEmpty(), "a refused message must not be proven")
    }

    @Test
    fun `deliveryPolicy should admit a refused message once it allows the sender`() {
        // given
        router.deliveryPolicy = DeliveryPolicy { _, _ -> false }
        val packed = packedMessage("later")
        deliverOpportunistically(packed)

        // when
        router.deliveryPolicy = DeliveryPolicy { _, _ -> true }
        val packet = deliverOpportunistically(packed)

        // then
        assertEquals("later", delivered.single().content)
        assertEquals(listOf(packet), proven)
    }

    @Test
    fun `deliveryPolicy should be given the source hash and the fields`() {
        // given
        var seenSource: ByteArray? = null
        var seenFields: Map<Int, Any>? = null
        router.deliveryPolicy = DeliveryPolicy { source, fields ->
            seenSource = source
            seenFields = fields
            true
        }

        // when
        deliverOpportunistically(packedMessage("fields", mutableMapOf(HOST_FIELD to "analog.test")))

        // then
        assertArrayEquals(senderDeliveryHash, seenSource)
        assertEquals("analog.test", seenFields?.get(HOST_FIELD))
    }

    @Test
    fun `deliveryPolicy should change nothing when none is set`() {
        // when
        val packet = deliverOpportunistically(packedMessage("open"))

        // then
        assertEquals("open", delivered.single().content)
        assertEquals(listOf(packet), proven)
    }

    @Test
    fun `deliveryPolicy should not be asked about a message with a bad signature`() {
        // given
        var asked = false
        router.deliveryPolicy = DeliveryPolicy { _, _ ->
            asked = true
            true
        }
        val packed = packedMessage("forged")
        packed[SIGNATURE_OFFSET] = (packed[SIGNATURE_OFFSET].toInt() xor 1).toByte()

        // when
        val packet = deliverOpportunistically(packed)

        // then
        assertFalse(asked, "the policy must see only a verified source")
        assertTrue(delivered.isEmpty())
        assertEquals(listOf(packet), proven, "a bad signature still proves, as python does")
    }

    @Test
    fun `registerDeliveryCallback should run after the proof on the opportunistic path`() {
        // when
        val packet = deliverOpportunistically(packedMessage("order"))

        // then
        assertEquals(listOf(PROOF, DELIVERED), events)
        assertEquals(listOf(packet), proven)
        assertEquals(DeliveryMethod.OPPORTUNISTIC, delivered.single().method)
    }

    @Test
    fun `registerDeliveryCallback should run after the proof on a delivery link`() {
        // when
        val packet = deliverOverLink(packedMessage("link"))

        // then
        assertEquals(listOf(PROOF, DELIVERED), events)
        assertEquals(listOf(packet), proven)
        assertEquals(DeliveryMethod.DIRECT, delivered.single().method)
    }

    @Test
    fun `registerDeliveryCallback should run once when a message arrives twice`() {
        // given
        val packed = packedMessage("twice")
        val first = deliverOpportunistically(packed)

        // when
        val second = deliverOpportunistically(packed)

        // then
        assertEquals(1, delivered.size)
        assertEquals(listOf(first, second), proven, "a duplicate still proves, as python does")
    }

    @Test
    fun `proves should be false only for a policy refusal`() {
        // given
        val message = LXMessage.unpackFromBytes(packedMessage("any"))!!

        // then
        assertTrue(InboundOutcome.Admitted(message).proves)
        assertTrue(InboundOutcome.Duplicate.proves)
        assertTrue(InboundOutcome.Rejected.proves)
        assertFalse(InboundOutcome.RefusedByPolicy.proves)
    }

    /** The wire bytes of a message from the sender to this router's delivery destination. */
    private fun packedMessage(content: String, fields: MutableMap<Int, Any> = mutableMapOf()): ByteArray {
        val source = Destination.create(
            identity = senderIdentity,
            direction = DestinationDirection.IN,
            type = DestinationType.SINGLE,
            appName = "lxmf",
            "delivery",
        )
        val destination = Destination.create(
            identity = ownIdentity,
            direction = DestinationDirection.OUT,
            type = DestinationType.SINGLE,
            appName = "lxmf",
            "delivery",
        )
        val message = LXMessage.create(
            destination = destination,
            source = source,
            content = content,
            title = "t",
            fields = fields,
        )

        return message.pack()
    }

    /**
     * Hands [packed] to the delivery destination's packet callback, as Transport does for an
     * opportunistic packet, whose data starts after the destination hash.
     *
     * @return the packet the router was given, which it proves or not.
     */
    private fun deliverOpportunistically(packed: ByteArray): Packet {
        val afterDestHash = packed.copyOfRange(LXMFConstants.DESTINATION_LENGTH, packed.size)
        val packet = Packet.createRaw(destinationHash = deliveryDestination.hash, data = afterDestHash)
        deliveryDestination.packetCallback!!.invoke(afterDestHash, packet)

        return packet
    }

    /**
     * Hands [packed] to the packet callback of a delivery link, as a link does for a message
     * small enough for one packet. The link is one the destination just accepted.
     *
     * @return the packet the router was given, which it proves or not.
     */
    private fun deliverOverLink(packed: ByteArray): Packet {
        val senderDestination = Destination.create(
            identity = senderIdentity,
            direction = DestinationDirection.OUT,
            type = DestinationType.SINGLE,
            appName = "lxmf",
            "delivery",
        )
        val link = Link.create(senderDestination)
        deliveryDestination.invokeLinkEstablished(link)

        val packet = Packet.createRaw(destinationHash = deliveryDestination.hash, data = packed)
        link.callbacks.packet!!.invoke(packed, packet)

        return packet
    }

    private companion object {
        const val PROOF = "proof"
        const val DELIVERED = "delivered"

        /** A field id outside LXMF's own. The policy must see its value as sent. */
        const val HOST_FIELD = 0xC0

        /** Where the signature starts on the wire: after the destination and source hashes. */
        const val SIGNATURE_OFFSET = 2 * LXMFConstants.DESTINATION_LENGTH
    }
}
