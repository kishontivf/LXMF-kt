package network.reticulum.lxmf

import network.reticulum.common.toHexString
import network.reticulum.identity.Identity
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import org.msgpack.core.MessagePack
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * [LXMRouter.handlePropagationAnnounce] reads a node's announce as LXMF-swift does: the legacy
 * flag, the timebase and the node state are required, and every later field is optional with a
 * default. Our own node announces six fields, without the metadata, and no Android phone knew it
 * while seven were required.
 */
@DisplayName("LXMRouter.handlePropagationAnnounce")
class PropagationAnnounceTest {

    private val router = LXMRouter(identity = Identity.create())
    private val node = Identity.create()

    @AfterEach
    fun teardown() {
        router.close()
    }

    @Test
    fun `handlePropagationAnnounce should accept a six-field announce with no name`() {
        // when
        router.handlePropagationAnnounce(NODE_HASH, node, announce(fields = 6))

        // then
        val stored = router.getPropagationNodes().single()
        assertEquals(NODE_HASH.toHexString(), stored.hexHash)
        assertNull(stored.displayName)
        assertEquals(TIMEBASE, stored.timebase)
        assertEquals(TRANSFER_LIMIT, stored.perTransferLimit)
        assertEquals(SYNC_LIMIT, stored.perSyncLimit)
        assertEquals(STAMP_COST, stored.stampCost)
        assertEquals(STAMP_FLEXIBILITY, stored.stampCostFlexibility)
        assertEquals(PEERING_COST, stored.peeringCost)
    }

    @Test
    fun `handlePropagationAnnounce should read the name from a seven-field announce`() {
        // when
        router.handlePropagationAnnounce(NODE_HASH, node, announce(fields = 7))

        // then
        val stored = router.getPropagationNodes().single()
        assertEquals(NAME, stored.displayName)
        assertEquals(STAMP_COST, stored.stampCost)
    }

    @Test
    fun `handlePropagationAnnounce should default the fields a three-field announce leaves out`() {
        // when
        router.handlePropagationAnnounce(NODE_HASH, node, announce(fields = 3))

        // then
        val stored = router.getPropagationNodes().single()
        assertEquals(TIMEBASE, stored.timebase)
        assertNull(stored.displayName)
        assertEquals(LXMFConstants.PROPAGATION_LIMIT_KB, stored.perTransferLimit)
        assertEquals(LXMFConstants.SYNC_LIMIT_KB, stored.perSyncLimit)
        assertEquals(LXMFConstants.PROPAGATION_COST, stored.stampCost)
        assertEquals(LXMFConstants.PROPAGATION_COST_FLEX, stored.stampCostFlexibility)
        assertEquals(LXMFConstants.PEERING_COST, stored.peeringCost)
    }

    @Test
    fun `handlePropagationAnnounce should refuse an announce with fewer than three fields`() {
        // when
        router.handlePropagationAnnounce(NODE_HASH, node, announce(fields = 2))

        // then
        assertTrue(router.getPropagationNodes().isEmpty())
    }

    /** An older node sends its stamp cost as one number rather than a list. */
    @Test
    fun `handlePropagationAnnounce should read a single stamp cost`() {
        // when
        router.handlePropagationAnnounce(NODE_HASH, node, announce(fields = 6, stampCostAsList = false))

        // then
        val stored = router.getPropagationNodes().single()
        assertEquals(STAMP_COST, stored.stampCost)
        assertEquals(LXMFConstants.PROPAGATION_COST_FLEX, stored.stampCostFlexibility)
    }

    /**
     * The first [fields] of a node's announce, packed as a node packs them:
     * `[legacy flag, timebase, node state, per-transfer limit, per-sync limit, stamp cost, metadata]`.
     */
    private fun announce(fields: Int, stampCostAsList: Boolean = true): ByteArray {
        val packer = MessagePack.newDefaultBufferPacker()
        val writers: List<() -> Unit> = listOf(
            { packer.packBoolean(false) },
            { packer.packLong(TIMEBASE) },
            { packer.packBoolean(true) },
            { packer.packInt(TRANSFER_LIMIT) },
            { packer.packInt(SYNC_LIMIT) },
            {
                if (stampCostAsList) {
                    packer.packArrayHeader(3)
                    packer.packInt(STAMP_COST)
                    packer.packInt(STAMP_FLEXIBILITY)
                    packer.packInt(PEERING_COST)
                } else {
                    packer.packInt(STAMP_COST)
                }
            },
            {
                val name = NAME.toByteArray(Charsets.UTF_8)
                packer.packMapHeader(1)
                packer.packInt(LXMFConstants.PN_META_NAME)
                packer.packBinaryHeader(name.size)
                packer.writePayload(name)
            },
        )

        packer.packArrayHeader(fields)
        writers.take(fields).forEach { write -> write() }
        packer.close()

        return packer.toByteArray()
    }

    private companion object {

        val NODE_HASH = ByteArray(16) { index -> index.toByte() }

        const val TIMEBASE = 1_700_000_000L
        const val TRANSFER_LIMIT = 512
        const val SYNC_LIMIT = 1_024
        const val STAMP_COST = 12
        const val STAMP_FLEXIBILITY = 2
        const val PEERING_COST = 20
        const val NAME = "Analog node"
    }
}
