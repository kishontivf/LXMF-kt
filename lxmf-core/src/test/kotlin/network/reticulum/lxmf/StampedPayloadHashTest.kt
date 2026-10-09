package network.reticulum.lxmf

import network.reticulum.common.DestinationDirection
import network.reticulum.common.DestinationType
import network.reticulum.crypto.Hashes
import network.reticulum.destination.Destination
import network.reticulum.identity.Identity
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import java.nio.ByteBuffer
import kotlin.test.assertContentEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

/**
 * A stamped message verifies against the payload bytes the sender hashed, whatever the sender's
 * encoding choices were.
 *
 * The sender hashes the payload without the stamp, then appends the stamp and sends. The
 * receiver needs those same bytes back to check the signature. Decoding the payload and
 * encoding it again drops a nil field value and turns a float32 into a float64. The bytes then
 * differ, and the signature fails to verify. iOS also writes its fields map in a random key
 * order, the reverse of this library's in the first test. These tests build each message by
 * hand, byte for byte, so nothing here depends on this library's own encoder.
 */
@DisplayName("LXMF stamped messages are hashed over the bytes the sender encoded")
class StampedPayloadHashTest {

    @Test
    fun `unpackFromBytes should verify a stamped message whose fields are in iOS's order`() {
        // given
        val sender = Sender()
        val fields = mapHeader(2) + customDataField() + customTypeField()
        val message = sender.stampedMessage(fields)

        // when
        val unpacked = assertNotNull(LXMessage.unpackFromBytes(message.bytes))

        // then
        assertTrue(unpacked.signatureValidated)
        assertContentEquals(message.hash, unpacked.hash)
        assertContentEquals(STAMP, unpacked.stamp)
    }

    @Test
    fun `unpackFromBytes should verify a stamped message with a nil field value`() {
        // given
        val sender = Sender()
        val fields = mapHeader(3) + customDataField() + customTypeField() + customMetaField(byteArrayOf(NIL))
        val message = sender.stampedMessage(fields)

        // when
        val unpacked = assertNotNull(LXMessage.unpackFromBytes(message.bytes))

        // then
        assertTrue(unpacked.signatureValidated)
        assertContentEquals(message.hash, unpacked.hash)
    }

    @Test
    fun `unpackFromBytes should verify a stamped message with a float32 field value`() {
        // given
        val sender = Sender()
        val fields = mapHeader(3) + customDataField() + customTypeField() + customMetaField(float32(1.5f))
        val message = sender.stampedMessage(fields)

        // when
        val unpacked = assertNotNull(LXMessage.unpackFromBytes(message.bytes))

        // then
        assertTrue(unpacked.signatureValidated)
        assertContentEquals(message.hash, unpacked.hash)
    }

    @Test
    fun `unpackFromBytes should verify a stamped message whose fields element is nil`() {
        // given
        val sender = Sender()
        val message = sender.stampedMessage(byteArrayOf(NIL))

        // when
        val unpacked = assertNotNull(LXMessage.unpackFromBytes(message.bytes))

        // then
        assertTrue(unpacked.signatureValidated)
        assertContentEquals(message.hash, unpacked.hash)
        assertTrue(unpacked.fields.isEmpty())
    }

    @Test
    fun `unpackFromBytes should verify a stamped message this library packed`() {
        // given
        val sender = Sender()
        val message = LXMessage.create(
            destination = sender.destination,
            source = sender.source,
            content = "",
            title = "",
            fields = mutableMapOf(
                LXMFConstants.FIELD_CUSTOM_TYPE to "ag",
                LXMFConstants.FIELD_CUSTOM_DATA to GROUP_PAYLOAD,
            ),
        )
        message.stamp = STAMP
        val packed = message.pack()

        // when
        val unpacked = assertNotNull(LXMessage.unpackFromBytes(packed))

        // then
        assertTrue(unpacked.signatureValidated)
        assertContentEquals(message.hash, unpacked.hash)
    }

    /** A sender the receiver knows, and the two addresses every message here carries. */
    private class Sender {

        private val identity = Identity.create()

        val source: Destination = Destination.create(
            identity = identity,
            direction = DestinationDirection.IN,
            type = DestinationType.SINGLE,
            appName = "lxmf",
            "delivery",
        )

        val destination: Destination = Destination.create(
            identity = Identity.create(),
            direction = DestinationDirection.OUT,
            type = DestinationType.SINGLE,
            appName = "lxmf",
            "delivery",
        )

        init {
            Identity.remember(
                packetHash = ByteArray(HASH_LENGTH),
                destHash = source.hash,
                publicKey = identity.getPublicKey(),
            )
        }

        /**
         * Signs a message exactly as LXMF does, then appends the stamp.
         *
         * The hash covers the destination, the source and the four-element payload. The
         * signature covers that and the hash. The sent payload is the same four elements with
         * the stamp as a fifth.
         *
         * @param fields the fields element, already encoded.
         * @return the wire bytes and the hash the sender signed.
         */
        fun stampedMessage(fields: ByteArray): SignedMessage {
            val hashedPart = destination.hash + source.hash + payload(fields, stamp = null)
            val hash = Hashes.fullHash(hashedPart)
            val signature = identity.sign(hashedPart + hash)
            val bytes = destination.hash + source.hash + signature + payload(fields, stamp = STAMP)

            return SignedMessage(bytes, hash)
        }
    }

    private class SignedMessage(val bytes: ByteArray, val hash: ByteArray)

    private companion object {

        /** Msgpack fixarray header for four elements. */
        const val ARRAY_OF_FOUR: Byte = 0x94.toByte()

        /** Msgpack fixarray header for five elements. */
        const val ARRAY_OF_FIVE: Byte = 0x95.toByte()

        /** Msgpack fixmap header for an empty map. A map of n entries, up to 15, is this plus n. */
        const val FIXMAP: Int = 0x80

        /** Msgpack fixstr header for an empty string. A string of n bytes, up to 31, is this plus n. */
        const val FIXSTR: Int = 0xa0

        /** Msgpack nil. */
        const val NIL: Byte = 0xc0.toByte()

        /** Msgpack bin 8 header, followed by a one-byte length. */
        const val BIN8: Byte = 0xc4.toByte()

        /** Msgpack float 32 header, followed by four bytes. */
        const val FLOAT32: Byte = 0xca.toByte()

        /** Msgpack float 64 header, followed by eight bytes. */
        const val FLOAT64: Byte = 0xcb.toByte()

        /** Msgpack uint 8 header, followed by one byte. The field keys 0xFB to 0xFD need it. */
        const val UINT8: Byte = 0xcc.toByte()

        /** The time the message was sent, in UNIX seconds. Any value does; it is only hashed. */
        const val TIMESTAMP = 1_790_000_000.5

        /** Length of a full SHA-256 hash, in bytes. `Identity.remember` wants a packet hash of this length. */
        const val HASH_LENGTH = 32

        /** Length of a stamp, in bytes: a full hash of the proof of work. */
        const val STAMP_LENGTH = 32

        /** A stamp of the right size. It is only carried; `unpackFromBytes` does not check it. */
        val STAMP = ByteArray(STAMP_LENGTH) { it.toByte() }

        /** Stands in for a group payload. Its content does not matter; it is only hashed. */
        val GROUP_PAYLOAD = byteArrayOf(1, 2, 3, 4, 5, 6, 7, 8)

        /** Field 0xFC holding [GROUP_PAYLOAD] as a bin. */
        fun customDataField(): ByteArray = field(LXMFConstants.FIELD_CUSTOM_DATA, bin(GROUP_PAYLOAD))

        /** Field 0xFB holding the str `"ag"`. */
        fun customTypeField(): ByteArray = field(LXMFConstants.FIELD_CUSTOM_TYPE, str("ag"))

        /** Field 0xFD holding an already encoded value. */
        fun customMetaField(value: ByteArray): ByteArray = field(LXMFConstants.FIELD_CUSTOM_META, value)

        /**
         * The payload as iOS sends a group copy: a float64 timestamp, an empty title, an empty
         * content, the given fields element, and the stamp when there is one.
         */
        fun payload(fields: ByteArray, stamp: ByteArray?): ByteArray {
            val header = if (stamp == null) ARRAY_OF_FOUR else ARRAY_OF_FIVE
            val timestamp = byteArrayOf(FLOAT64) + ByteBuffer.allocate(Double.SIZE_BYTES).putDouble(TIMESTAMP).array()
            val stampElement = stamp?.let { bin(it) } ?: ByteArray(0)

            return byteArrayOf(header) + timestamp + bin(ByteArray(0)) + bin(ByteArray(0)) + fields + stampElement
        }

        fun mapHeader(entries: Int): ByteArray = byteArrayOf((FIXMAP + entries).toByte())

        fun field(key: Int, value: ByteArray): ByteArray = byteArrayOf(UINT8, key.toByte()) + value

        fun bin(bytes: ByteArray): ByteArray = byteArrayOf(BIN8, bytes.size.toByte()) + bytes

        fun str(text: String): ByteArray = byteArrayOf((FIXSTR + text.length).toByte()) + text.toByteArray()

        fun float32(value: Float): ByteArray =
            byteArrayOf(FLOAT32) + ByteBuffer.allocate(Float.SIZE_BYTES).putFloat(value).array()
    }
}
