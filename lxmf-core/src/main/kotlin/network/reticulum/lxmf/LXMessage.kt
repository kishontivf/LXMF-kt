package network.reticulum.lxmf

import network.reticulum.common.toHexString
import network.reticulum.crypto.Hashes
import network.reticulum.destination.Destination
import network.reticulum.identity.Identity
import org.msgpack.core.MessagePack
import java.io.ByteArrayOutputStream
import java.util.Base64
import network.reticulum.common.RnsLog

/**
 * LXMF Message class.
 *
 * Represents a message in the LXMF format with support for packing/unpacking
 * that is byte-perfect compatible with Python LXMF.
 *
 * Wire format:
 * ```
 * [0:16]   Destination hash (16 bytes)
 * [16:32]  Source hash (16 bytes)
 * [32:96]  Ed25519 signature (64 bytes)
 * [96:]    Msgpack payload
 * ```
 *
 * Payload structure (msgpack list):
 * ```
 * [0] timestamp  - float64 (UNIX epoch seconds)
 * [1] title      - bytes (UTF-8)
 * [2] content    - bytes (UTF-8)
 * [3] fields     - dict (extensible)
 * [4] stamp      - bytes (optional, 32 bytes proof-of-work)
 * ```
 */
class LXMessage private constructor(
    /** Destination for this message */
    val destination: Destination?,
    /** Source destination (sender) */
    val source: Destination?,
    /** Destination hash (always available even if destination is null) */
    val destinationHash: ByteArray,
    /** Source hash (always available even if source is null) */
    val sourceHash: ByteArray,
    /** Message title */
    var title: String,
    /** Message content */
    var content: String,
    /** Extended fields dictionary */
    val fields: MutableMap<Int, Any> = mutableMapOf(),
    /** Desired delivery method */
    var desiredMethod: DeliveryMethod? = null,
) {
    // ===== Message Identification =====

    /** Full message hash (32 bytes SHA-256) */
    var hash: ByteArray? = null
        private set

    /** Message ID (same as hash) */
    val messageId: ByteArray?
        get() = hash

    /** Transient ID for propagation (hash of encrypted data) */
    var transientId: ByteArray? = null
        private set

    // ===== State and Flags =====

    /**
     * Current message state.
     *
     * `@Volatile` because it is written from several threads: the router's processing
     * coroutines, packet receipt callbacks, Resource callbacks, and [LXMRouter.cancelOutbound]
     * from the caller's own coroutine. See `port-deviations.md`.
     */
    @Volatile var state: MessageState = MessageState.GENERATING

    /** Message representation (PACKET or RESOURCE) */
    var representation: MessageRepresentation = MessageRepresentation.UNKNOWN

    /** Actual delivery method used */
    var method: DeliveryMethod? = null

    /** Whether this is an incoming message */
    var incoming: Boolean = false

    /** Whether the signature has been validated */
    var signatureValidated: Boolean = false

    /** Reason why signature validation failed */
    var unverifiedReason: UnverifiedReason? = null

    // ===== Timestamps =====

    /** Message timestamp (UNIX epoch seconds as Double) */
    var timestamp: Double? = null

    // ===== Packed Data =====

    /** Packed message bytes (wire format) */
    var packed: ByteArray? = null
        private set

    /** Size of packed message */
    val packedSize: Int
        get() = packed?.size ?: 0

    /** Ed25519 signature (64 bytes) */
    var signature: ByteArray? = null
        private set

    /** Proof-of-work stamp (32 bytes, optional) */
    var stamp: ByteArray? = null

    /** Whether the stamp has been validated */
    var stampValid: Boolean = false

    /** Whether the stamp has been checked */
    var stampChecked: Boolean = false

    /** Validated stamp value (leading zero bits), or null if not checked */
    var stampValue: Int? = null

    /** Required stamp cost for this message */
    var stampCost: Int? = null

    /** Outbound ticket for stamp bypass */
    var outboundTicket: ByteArray? = null

    /** Whether to include a ticket in this message */
    var includeTicket: Boolean = false

    /** Whether to defer stamp generation (compute later in background) */
    var deferStamp: Boolean = false

    /** Packed bytes for PAPER delivery (destHash + encrypted rest) */
    var paperPacked: ByteArray? = null
        private set

    // ===== Encryption State =====

    /** Whether message was transport-encrypted */
    var transportEncrypted: Boolean = false

    /** Description of transport encryption used */
    var transportEncryption: String? = null

    /**
     * Progress of message delivery (0.0 to 1.0).
     *
     * `@Volatile` because writers and readers run on different threads.
     * Writers: `LXMRouter.processOpportunisticDelivery` (LXMRouter.kt:739,
     * 755), `LXMRouter.sendViaPropagation`'s Resource progressCallback
     * (LXMRouter.kt:1258), and `LXMRouter.sendViaLink`'s Resource
     * progressCallback + completion callback (LXMRouter.kt:1335, 1340) —
     * all dispatched from `processingScope` coroutines or RNS Resource
     * background threads. Readers: any caller polling progress for UI
     * display, plus the conformance bridge's `cmdLxmfGetMessageProgress`
     * (Main.kt:740) reading from the bridge's JSON-RPC dispatch thread.
     *
     * Without `@Volatile`, the JLS allows non-volatile `double` reads to
     * tear (§17.7) and offers no happens-before edge between the write
     * and a cross-thread read — visibility of the latest value is
     * implementation-defined. Python's GIL gives this for free; on JVM
     * `@Volatile` is the direct equivalent.
     */
    @Volatile var progress: Double = 0.0

    // ===== Callbacks =====

    /** Callback when message is delivered */
    var deliveryCallback: ((LXMessage) -> Unit)? = null

    /** Callback when message delivery fails */
    var failedCallback: ((LXMessage) -> Unit)? = null

    // ===== Delivery Tracking =====

    /** Number of delivery attempts made */
    var deliveryAttempts: Int = 0

    /** Next delivery attempt timestamp (milliseconds) */
    var nextDeliveryAttempt: Long? = null

    /**
     * When [LXMRouter.handleOutbound] last took this message, in epoch milliseconds.
     *
     * Null until then. The router's age limit counts from this moment, not from [timestamp], so
     * a message handed over again after a restart keeps its timestamp and its hash and still gets
     * a fresh limit. Not part of the wire format.
     */
    var handedOverAt: Long? = null

    /**
     * Whether a path re-request has already been issued for a CLOSED delivery
     * link that never activated. Transient delivery-state — NOT part of the
     * packed wire format. Mirrors Python LXMF's dynamic `path_request_retried`
     * attribute (LXMRouter.py:2615-2618), which gates the never-activated retry
     * to exactly once.
     */
    var pathRequestRetried: Boolean = false

    // ===== Receive-time Packet Metadata =====
    //
    // The following fields are populated from the delivering Reticulum packet
    // when this LXMessage is constructed on the receive side. They are only
    // meaningful for live, in-path delivery (OPPORTUNISTIC and DIRECT);
    // outgoing messages do not carry them, and messages pulled from a
    // propagation node are intentionally left null because the original
    // in-path packet context is lost (the values would reflect the
    // propagation-node sync link, not the originating sender — which would
    // be misleading).

    /**
     * RSSI of the delivering packet (signed integer, typically dBm).
     *
     * Null for outgoing messages and for messages fetched from a propagation
     * node. For Resource-delivered (multi-packet) messages, this reflects the
     * phy stats of the link at the moment the Resource assembly concluded
     * (i.e. the final constituent packet). Requires the underlying Link to
     * have `trackPhyStats(true)` enabled for Resource-delivered messages; for
     * single-packet paths the value is copied from the delivering `Packet`
     * directly and is available unconditionally.
     */
    var receivedRssi: Int? = null

    /**
     * SNR of the delivering packet.
     *
     * Null for outgoing messages and for messages fetched from a propagation
     * node. See [receivedRssi] for semantics on Resource-delivered messages.
     */
    var receivedSnr: Float? = null

    /**
     * Hash of the interface the delivering packet arrived on.
     *
     * Null for outgoing messages and for messages fetched from a propagation
     * node. For Resource-delivered messages, reflects the interface the link
     * was attached to.
     */
    var receivingInterfaceHash: ByteArray? = null

    /**
     * Number of hops the delivering packet traveled to reach us.
     *
     * Null for outgoing messages and for messages fetched from a propagation
     * node. For Resource-delivered messages, reflects the link's expected
     * hop count (established at link-setup time), which is the correct hop
     * count for the Resource because every Resource constituent packet
     * travels the same hop path as the link itself.
     */
    var receivedHopCount: Int? = null

    /**
     * Pack the message into wire format.
     *
     * This creates the packed byte array that can be sent over the network.
     * The packing process:
     * 1. Create payload list: [timestamp, title, content, fields]
     * 2. Compute hash: SHA256(destHash + sourceHash + msgpack(payload))
     * 3. Sign: Ed25519(hashedPart + hash)
     * 4. Pack: destHash + sourceHash + signature + msgpack(payload)
     *
     * @return The packed message bytes
     * @throws IllegalStateException if source has no private key for signing
     */
    fun pack(): ByteArray {
        if (packed != null) {
            return packed!!
        }

        // Set timestamp if not set
        if (timestamp == null) {
            timestamp = System.currentTimeMillis() / 1000.0
        }

        // Get source identity for signing
        val sourceIdentity =
            source?.identity
                ?: throw IllegalStateException("Cannot pack message without source identity")
        require(sourceIdentity.hasPrivateKey) { "Cannot pack message: source has no private key" }

        // Build payload: [timestamp, title, content, fields]
        val payloadBytes = packPayload(timestamp!!, title, content, fields, stamp)

        // Build hashed part: destHash + sourceHash + msgpack(payload without stamp)
        val payloadWithoutStamp = packPayload(timestamp!!, title, content, fields, null)
        val hashedPart = destinationHash + sourceHash + payloadWithoutStamp

        // Compute message hash
        hash = Hashes.fullHash(hashedPart)

        // Build signed part: hashedPart + hash
        val signedPart = hashedPart + hash!!

        // Sign the message
        signature = sourceIdentity.sign(signedPart)
        signatureValidated = true

        // Build packed message: destHash + sourceHash + signature + payload
        packed = destinationHash + sourceHash + signature!! + payloadBytes

        // Determine delivery method and representation
        determineDeliveryMethod()

        return packed!!
    }

    /**
     * Re-pack the message with an updated stamp.
     *
     * Called after deferred stamp generation to update the packed bytes
     * with the newly generated stamp. The hash and signature don't change
     * because stamp is not included in the hashed/signed portion.
     */
    fun repackWithStamp() {
        if (stamp == null || hash == null || signature == null) return

        val payloadBytes = packPayload(timestamp!!, title, content, fields, stamp)
        packed = destinationHash + sourceHash + signature!! + payloadBytes

        determineDeliveryMethod()
    }

    /**
     * Determine the delivery method and representation based on message size.
     */
    private fun determineDeliveryMethod() {
        val contentSize = packed!!.size - LXMFConstants.LXMF_OVERHEAD

        // Measured differently from `contentSize`, and only for the opportunistic decision. The
        // expression is fixed by [LXMFConstants.ENCRYPTED_PACKET_MAX_PAYLOAD], which every client
        // holds to one value so that a given message never takes a link on one device and a single
        // packet on another.
        val opportunisticPayloadSize =
            packed!!.size - (2 * LXMFConstants.DESTINATION_LENGTH + LXMFConstants.SIGNATURE_LENGTH)

        // Default to DIRECT if not specified
        if (desiredMethod == null) {
            desiredMethod = DeliveryMethod.DIRECT
        }

        when (desiredMethod) {
            DeliveryMethod.OPPORTUNISTIC -> {
                if (opportunisticPayloadSize > LXMFConstants.ENCRYPTED_PACKET_MAX_PAYLOAD) {
                    // Fall back to DIRECT for large messages
                    RnsLog.debug("LXMessage") { "Opportunistic delivery requested but content too large ($opportunisticPayloadSize bytes), falling back to DIRECT" }
                    desiredMethod = DeliveryMethod.DIRECT
                    method = DeliveryMethod.DIRECT
                    representation =
                        if (contentSize <= LXMFConstants.LINK_PACKET_MAX_CONTENT) {
                            MessageRepresentation.PACKET
                        } else {
                            MessageRepresentation.RESOURCE
                        }
                } else {
                    method = DeliveryMethod.OPPORTUNISTIC
                    representation = MessageRepresentation.PACKET
                }
            }
            DeliveryMethod.DIRECT -> {
                method = DeliveryMethod.DIRECT
                representation =
                    if (contentSize <= LXMFConstants.LINK_PACKET_MAX_CONTENT) {
                        MessageRepresentation.PACKET
                    } else {
                        MessageRepresentation.RESOURCE
                    }
            }
            DeliveryMethod.PROPAGATED -> {
                method = DeliveryMethod.PROPAGATED
                // Propagated messages have additional encryption overhead
                representation = MessageRepresentation.RESOURCE // Conservative default
            }
            DeliveryMethod.PAPER -> {
                method = DeliveryMethod.PAPER
                representation = MessageRepresentation.PACKET
            }
            null -> {
                method = DeliveryMethod.DIRECT
                representation = MessageRepresentation.PACKET
            }
        }
    }

    /**
     * Pack payload into msgpack format.
     */
    private fun packPayload(
        timestamp: Double,
        title: String,
        content: String,
        fields: Map<Int, Any>,
        stamp: ByteArray?,
    ): ByteArray {
        val buffer = ByteArrayOutputStream()
        val packer = MessagePack.newDefaultPacker(buffer)

        // Pack as list with 4 or 5 elements
        val elementCount = if (stamp != null) 5 else 4
        packer.packArrayHeader(elementCount)

        // [0] timestamp as float64
        packer.packDouble(timestamp)

        // [1] title as bytes
        val titleBytes = title.toByteArray(Charsets.UTF_8)
        packer.packBinaryHeader(titleBytes.size)
        packer.writePayload(titleBytes)

        // [2] content as bytes
        val contentBytes = content.toByteArray(Charsets.UTF_8)
        packer.packBinaryHeader(contentBytes.size)
        packer.writePayload(contentBytes)

        // [3] fields as map
        packer.packMapHeader(fields.size)
        for ((key, value) in fields) {
            packer.packInt(key)
            packValue(packer, value)
        }

        // [4] stamp (optional)
        if (stamp != null) {
            packer.packBinaryHeader(stamp.size)
            packer.writePayload(stamp)
        }

        packer.close()
        return buffer.toByteArray()
    }

    /**
     * Pack a value into msgpack format (recursive for nested structures).
     */
    private fun packValue(
        packer: org.msgpack.core.MessagePacker,
        value: Any,
    ) {
        when (value) {
            is ByteArray -> {
                packer.packBinaryHeader(value.size)
                packer.writePayload(value)
            }
            is String -> packer.packString(value)
            is Int -> packer.packInt(value)
            is Long -> packer.packLong(value)
            is Double -> packer.packDouble(value)
            is Float -> packer.packFloat(value)
            is Boolean -> packer.packBoolean(value)
            is List<*> -> {
                packer.packArrayHeader(value.size)
                for (item in value) {
                    if (item != null) {
                        packValue(packer, item)
                    } else {
                        packer.packNil()
                    }
                }
            }
            is Map<*, *> -> {
                packer.packMapHeader(value.size)
                for ((k, v) in value) {
                    if (k != null) {
                        packValue(packer, k)
                    } else {
                        packer.packNil()
                    }
                    if (v != null) {
                        packValue(packer, v)
                    } else {
                        packer.packNil()
                    }
                }
            }
            else -> {
                // Default to string representation
                val str = value.toString().toByteArray(Charsets.UTF_8)
                packer.packBinaryHeader(str.size)
                packer.writePayload(str)
            }
        }
    }

    /**
     * Get title as bytes (UTF-8).
     */
    fun getTitleBytes(): ByteArray = title.toByteArray(Charsets.UTF_8)

    /**
     * Get content as bytes (UTF-8).
     */
    fun getContentBytes(): ByteArray = content.toByteArray(Charsets.UTF_8)

    /**
     * Set title from bytes.
     */
    fun setTitleFromBytes(bytes: ByteArray) {
        title = bytes.toString(Charsets.UTF_8)
    }

    /**
     * Set content from bytes.
     */
    fun setContentFromBytes(bytes: ByteArray) {
        content = bytes.toString(Charsets.UTF_8)
    }

    /**
     * Validate the stamp on this message.
     *
     * Matches Python LXMessage.validate_stamp() (lines 279-299):
     * 1. Ticket path: check if stamp == truncatedHash(ticket + messageId)
     * 2. Normal path: use LXStamper to validate proof-of-work
     *
     * @param targetCost Required stamp cost (leading zero bits)
     * @param tickets List of valid inbound tickets, or null
     * @return True if stamp is valid
     */
    fun validateStamp(
        targetCost: Int,
        tickets: List<ByteArray>? = null,
    ): Boolean {
        val msgHash = hash ?: return false
        val msgStamp = stamp

        stampChecked = true

        // Ticket path: check if stamp matches any ticket
        if (msgStamp != null && tickets != null) {
            for (ticket in tickets) {
                val ticketStamp = Hashes.truncatedHash(ticket + msgHash)
                if (msgStamp.contentEquals(ticketStamp)) {
                    stampValid = true
                    stampValue = LXMFConstants.COST_TICKET
                    return true
                }
            }
        }

        // Normal path: validate proof-of-work stamp
        if (msgStamp == null) {
            stampValid = false
            stampValue = null
            return false
        }

        val valid = LXStamper.validateStamp(msgStamp, msgHash, targetCost)
        stampValid = valid
        if (valid) {
            stampValue = LXStamper.getStampValue(msgStamp, msgHash)
        } else {
            stampValue = null
        }
        return valid
    }

    /**
     * Get or generate the stamp for this message.
     *
     * Matches Python LXMessage.get_stamp() (lines 304-332):
     * 1. Ticket path: if outboundTicket set, return truncatedHash(ticket + messageId)
     * 2. No cost: if stampCost null, return null
     * 3. Cached: if stamp already set, return it
     * 4. Generate: use LXStamper.generateStamp()
     *
     * @return Stamp bytes, or null if no stamp needed
     */
    suspend fun getStamp(): ByteArray? {
        val msgHash = hash ?: return null

        // Ticket path
        val ticket = outboundTicket
        if (ticket != null) {
            val ticketStamp = Hashes.truncatedHash(ticket + msgHash)
            stamp = ticketStamp
            stampCost = null
            return ticketStamp
        }

        // No cost required
        val cost = stampCost ?: return null

        // Cached stamp
        if (stamp != null) return stamp

        // Generate stamp
        val result = LXStamper.generateStampWithWorkblock(msgHash, cost)
        stamp = result.stamp
        return result.stamp
    }

    /**
     * Encode this message as a paper delivery URI (lxm://...).
     *
     * Matches Python LXMessage.as_uri() (lines 685-703):
     * 1. Pack message if not already packed
     * 2. Encrypt everything after dest hash for the destination
     * 3. Prepend dest hash to get paper_packed
     * 4. Base64url-encode without padding
     * 5. Prepend "lxm://"
     *
     * @return The lxm:// URI string
     */
    fun asUri(): String {
        if (packed == null) {
            pack()
        }

        val pp =
            paperPacked
                ?: throw IllegalStateException("Paper packing not done — call packForPaper() first or use PAPER delivery method")

        val encoded = Base64.getUrlEncoder().withoutPadding().encodeToString(pp)
        return "${URI_SCHEMA}://$encoded"
    }

    /**
     * Pack message for PAPER delivery.
     *
     * Encrypts message content for the destination and prepends the destination hash.
     * Must be called before asUri().
     *
     * @throws IllegalStateException if destination is null or has no identity
     */
    fun packForPaper() {
        if (packed == null) {
            pack()
        }

        val dest =
            destination
                ?: throw IllegalStateException("Cannot pack for paper without destination")

        val packedData = packed!!
        val plainData = packedData.copyOfRange(LXMFConstants.DESTINATION_LENGTH, packedData.size)
        val encryptedData = dest.encrypt(plainData)
        paperPacked = packedData.copyOfRange(0, LXMFConstants.DESTINATION_LENGTH) + encryptedData

        method = DeliveryMethod.PAPER
        representation = MessageRepresentation.PACKET
    }

    override fun toString(): String {
        val hashStr = hash?.toHexString()?.take(12) ?: "unpacked"
        return "<LXMessage $hashStr>"
    }

    companion object {
        /** URI schema prefix */
        const val URI_SCHEMA = "lxm"

        /**
         * Create a new outbound LXMF message.
         *
         * @param destination The destination to send to
         * @param source The source destination (sender)
         * @param content Message content
         * @param title Message title (default empty)
         * @param fields Extended fields (default empty)
         * @param desiredMethod Desired delivery method (default DIRECT)
         * @return New LXMessage instance
         */
        fun create(
            destination: Destination,
            source: Destination,
            content: String,
            title: String = "",
            fields: MutableMap<Int, Any> = mutableMapOf(),
            desiredMethod: DeliveryMethod? = DeliveryMethod.DIRECT,
        ): LXMessage =
            LXMessage(
                destination = destination,
                source = source,
                destinationHash = destination.hash,
                sourceHash = source.hash,
                title = title,
                content = content,
                fields = fields,
                desiredMethod = desiredMethod,
            )

        /**
         * Unpack an LXMF message from wire format bytes.
         *
         * Wire format:
         * ```
         * [0:16]   Destination hash
         * [16:32]  Source hash
         * [32:96]  Signature
         * [96:]    Msgpack payload
         * ```
         *
         * @param lxmfBytes The packed message bytes
         * @param originalMethod The original delivery method (optional)
         * @return Unpacked LXMessage, or null if unpacking fails
         */
        fun unpackFromBytes(
            lxmfBytes: ByteArray,
            originalMethod: DeliveryMethod? = null,
        ): LXMessage? {
            try {
                // Minimum size: dest_hash (16) + source_hash (16) + signature (64) + some payload
                val minHeaderSize = 2 * LXMFConstants.DESTINATION_LENGTH + LXMFConstants.SIGNATURE_LENGTH
                if (lxmfBytes.size <= minHeaderSize) {
                    RnsLog.debug("LXMessage") { "LXMF message too small: ${lxmfBytes.size} bytes (need > $minHeaderSize)" }
                    return null
                }

                // Extract fixed-length fields
                val destinationHash = lxmfBytes.copyOfRange(0, LXMFConstants.DESTINATION_LENGTH)
                val sourceHash =
                    lxmfBytes.copyOfRange(
                        LXMFConstants.DESTINATION_LENGTH,
                        2 * LXMFConstants.DESTINATION_LENGTH,
                    )
                val signature =
                    lxmfBytes.copyOfRange(
                        2 * LXMFConstants.DESTINATION_LENGTH,
                        2 * LXMFConstants.DESTINATION_LENGTH + LXMFConstants.SIGNATURE_LENGTH,
                    )
                val packedPayload =
                    lxmfBytes.copyOfRange(
                        2 * LXMFConstants.DESTINATION_LENGTH + LXMFConstants.SIGNATURE_LENGTH,
                        lxmfBytes.size,
                    )

                // Unpack msgpack payload
                val unpacker = MessagePack.newDefaultUnpacker(packedPayload)
                val arraySize = unpacker.unpackArrayHeader()

                // Where the first element starts, so hashedPayload can keep the elements as
                // received and only replace the array header.
                val firstElement = unpacker.totalReadBytes.toInt()

                if (arraySize < 4) {
                    RnsLog.warn("LXMessage") { "Invalid LXMF payload: expected at least 4 elements, got $arraySize" }
                    return null
                }

                // [0] timestamp
                val timestamp = unpacker.unpackDouble()

                // [1] title
                val titleLen = unpacker.unpackBinaryHeader()
                val titleBytes = ByteArray(titleLen)
                unpacker.readPayload(titleBytes)

                // [2] content
                val contentLen = unpacker.unpackBinaryHeader()
                val contentBytes = ByteArray(contentLen)
                unpacker.readPayload(contentBytes)

                // [3] fields — may be msgpack Nil (interop: other LXMF implementations and
                // python's `set_fields(None)` both produce Nil here; python tolerates this on
                // unpack via LXMessage.py:755 + set_fields() at LXMessage.py:220-224
                // which accepts None and normalizes to {}).
                // tryUnpackNil() peek-and-consumes in one call; if it returns true the
                // Nil byte is already consumed so no follow-up unpackNil() is needed.
                val fields =
                    if (unpacker.tryUnpackNil()) {
                        mutableMapOf()
                    } else {
                        unpackFields(unpacker)
                    }

                // Where the stamp starts, so hashedPayload can cut it out of the received bytes.
                val stampElement = unpacker.totalReadBytes.toInt()

                // [4] stamp (optional)
                val stamp: ByteArray? =
                    if (arraySize > 4) {
                        val stampLen = unpacker.unpackBinaryHeader()
                        val stampBytes = ByteArray(stampLen)
                        unpacker.readPayload(stampBytes)
                        stampBytes
                    } else {
                        null
                    }

                unpacker.close()

                // Python (LXMessage.py:742-747) decodes the payload and packs the first four
                // elements again to strip the stamp. On the JVM that round trip changes the
                // bytes, so this port cuts the stamp out of the received bytes instead. See
                // hashedPayload, and port-deviations.md.
                val payloadWithoutStamp =
                    if (stamp == null) {
                        packedPayload
                    } else {
                        hashedPayload(packedPayload, firstElement, stampElement)
                    }

                // Build hashed part
                val hashedPart = destinationHash + sourceHash + payloadWithoutStamp

                // Compute message hash
                val messageHash = Hashes.fullHash(hashedPart)

                // Build signed part
                val signedPart = hashedPart + messageHash

                // Try to recall identities
                val destinationIdentity = Identity.recall(destinationHash)
                val sourceIdentity = Identity.recall(sourceHash)

                // Create destinations if identities are known
                val destination =
                    if (destinationIdentity != null) {
                        // Note: We'd need to create a destination here, but for incoming
                        // messages we typically don't need the full destination object
                        null
                    } else {
                        null
                    }

                val source =
                    if (sourceIdentity != null) {
                        null
                    } else {
                        null
                    }

                // Create message
                val message =
                    LXMessage(
                        destination = destination,
                        source = source,
                        destinationHash = destinationHash,
                        sourceHash = sourceHash,
                        title = titleBytes.toString(Charsets.UTF_8),
                        content = contentBytes.toString(Charsets.UTF_8),
                        fields = fields,
                        desiredMethod = originalMethod,
                    )

                message.hash = messageHash
                message.signature = signature
                message.stamp = stamp
                message.incoming = true
                message.timestamp = timestamp
                message.packed = lxmfBytes

                // Validate signature if source identity is known
                if (sourceIdentity != null) {
                    try {
                        if (sourceIdentity.validate(signature, signedPart)) {
                            message.signatureValidated = true
                        } else {
                            message.signatureValidated = false
                            message.unverifiedReason = UnverifiedReason.SIGNATURE_INVALID
                        }
                    } catch (e: Exception) {
                        message.signatureValidated = false
                        RnsLog.error("LXMessage") { "Error validating LXMF signature: ${e.message}" }
                    }
                } else {
                    message.signatureValidated = false
                    message.unverifiedReason = UnverifiedReason.SOURCE_UNKNOWN
                    RnsLog.error("LXMessage") { "Cannot validate LXMF signature: source identity unknown" }
                }

                return message
            } catch (e: Exception) {
                RnsLog.error("LXMessage") { "Error unpacking LXMF message: ${e.message}" }
                e.printStackTrace()
                return null
            }
        }

        /**
         * Unpack fields map from msgpack.
         */
        private fun unpackFields(unpacker: org.msgpack.core.MessageUnpacker): MutableMap<Int, Any> {
            val fields = mutableMapOf<Int, Any>()
            val mapSize = unpacker.unpackMapHeader()

            repeat(mapSize) {
                val key = unpacker.unpackInt()
                val value = unpackValue(unpacker)
                if (value != null) {
                    fields[key] = value
                }
            }

            return fields
        }

        /**
         * Unpack a value from msgpack.
         */
        private fun unpackValue(unpacker: org.msgpack.core.MessageUnpacker): Any? {
            val format = unpacker.nextFormat
            val valueType = format.valueType
            return when (valueType.name) {
                "NIL" -> {
                    unpacker.unpackNil()
                    null
                }
                "BOOLEAN" -> unpacker.unpackBoolean()
                "INTEGER" -> unpacker.unpackLong()
                "FLOAT" -> unpacker.unpackDouble()
                "STRING" -> unpacker.unpackString()
                "BINARY" -> {
                    val len = unpacker.unpackBinaryHeader()
                    val bytes = ByteArray(len)
                    unpacker.readPayload(bytes)
                    bytes
                }
                "ARRAY" -> {
                    val size = unpacker.unpackArrayHeader()
                    val list = mutableListOf<Any?>()
                    repeat(size) {
                        list.add(unpackValue(unpacker))
                    }
                    list
                }
                "MAP" -> {
                    val size = unpacker.unpackMapHeader()
                    val map = mutableMapOf<Any?, Any?>()
                    repeat(size) {
                        val k = unpackValue(unpacker)
                        val v = unpackValue(unpacker)
                        map[k] = v
                    }
                    map
                }
                "EXTENSION" -> {
                    unpacker.skipValue()
                    null
                }
                else -> {
                    unpacker.skipValue()
                    null
                }
            }
        }

        /**
         * Returns the payload bytes the sender hashed: the received bytes with the stamp cut out.
         *
         * The sender hashed a four-element array, then sent the same elements with the stamp as
         * a fifth. So the hashed bytes are a four-element header, then the received bytes of the
         * first four elements, exactly as the sender encoded them. Decoding those elements and
         * encoding them again is not the same thing. A nil value is dropped, and a float32
         * becomes a float64. The hash would then differ from the one the sender signed, and a
         * genuine signature would fail to verify. LXMF-swift cuts the bytes the same way in
         * `hashedPayloadBytes`.
         *
         * @param packedPayload the received payload, stamp included.
         * @param firstElement offset of the first element, just after the array header.
         * @param stampElement offset of the stamp element. Everything from here on is cut.
         * @return the four-element payload the sender hashed.
         */
        private fun hashedPayload(packedPayload: ByteArray, firstElement: Int, stampElement: Int): ByteArray =
            byteArrayOf(FOUR_ELEMENT_ARRAY) + packedPayload.copyOfRange(firstElement, stampElement)

        /** Msgpack fixarray header for four elements, which every payload without a stamp starts with. */
        private const val FOUR_ELEMENT_ARRAY: Byte = 0x94.toByte()
    }
}
