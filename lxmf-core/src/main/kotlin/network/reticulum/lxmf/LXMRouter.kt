package network.reticulum.lxmf

import kotlinx.coroutines.*
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import network.reticulum.common.DestinationDirection
import network.reticulum.common.DestinationType
import network.reticulum.common.PacketContext
import network.reticulum.common.PacketType
import network.reticulum.common.RnsConstants
import network.reticulum.common.TransportType
import network.reticulum.common.toHexString
import network.reticulum.crypto.Hashes
import network.reticulum.destination.Destination
import network.reticulum.identity.Identity
import network.reticulum.link.Link
import network.reticulum.link.LinkConstants
import network.reticulum.packet.Packet
import network.reticulum.resource.Resource
import network.reticulum.resource.ResourceAdvertisement
import network.reticulum.transport.AnnounceHandler
import network.reticulum.transport.Transport
import org.msgpack.core.MessagePack
import java.io.ByteArrayOutputStream
import java.io.File
import java.security.SecureRandom
import java.util.Base64
import java.util.concurrent.ConcurrentHashMap
import network.reticulum.common.RnsLog

/**
 * LXMF Message Router.
 *
 * Handles sending and receiving LXMF messages with support for multiple
 * delivery methods: OPPORTUNISTIC, DIRECT, and PROPAGATED.
 *
 * This is a Kotlin port of Python LXMF's LXMRouter class.
 */
class LXMRouter(
    /** Identity for this router (optional, for creating destinations) */
    private val identity: Identity? = null,
    /** Storage path for router data */
    private val storagePath: String? = null,
    /** Whether to automatically peer with propagation nodes */
    private val autopeer: Boolean = true,
) : AutoCloseable {
    // ===== Configuration Constants =====

    companion object {
        /**
         * Attempts spent on one route before that route is treated as the problem.
         *
         * **This is a budget, not a death sentence**, and that is the whole difference between this
         * router's retry model and Python's. Python fails the message here. We assume instead that
         * a message which has failed this many times is being sent down a path that no longer
         * exists — the common case on a mesh, where a peer moves between carriers and leaves a
         * stale entry behind — so we throw the *path* away, ask for a fresh one, and hand the
         * message its attempts back. The only thing that ever fails a message outright is
         * [MAX_OUTBOUND_AGE].
         *
         * Higher than Python's five because these attempts are cheap now: they are spread by
         * [RetryBackoff] instead of a flat ten seconds, so eight of them still fit inside the first
         * few minutes.
         */
        const val MAX_DELIVERY_ATTEMPTS = 8

        /**
         * Processing loop interval in milliseconds.
         *
         * A quarter of Python's four seconds. The loop no longer decides *when* a message is due —
         * each message carries its own next-attempt time — so this only sets how precisely those
         * times are honoured. At four seconds a message due two seconds from now waited four, which
         * on a two-hop exchange was most of the delay a person could feel.
         */
        const val PROCESSING_INTERVAL = 1000L

        /**
         * Wait time between delivery retries in milliseconds.
         *
         * Retained for callers that want a plain fixed wait; the outbound path itself schedules
         * through [RetryBackoff] instead.
         */
        const val DELIVERY_RETRY_WAIT = 10000L

        /**
         * Wait time for path discovery in milliseconds.
         *
         * Equal to Reticulum's own `PATH_REQUEST_TIMEOUT`, which is what actually decides when an
         * unanswered request is dead. Python waits seven seconds and so gives up on a path request
         * that Reticulum is still waiting on: the retry fires while the answer is still in flight,
         * which then costs another request.
         */
        const val PATH_REQUEST_WAIT = 15000L

        /**
         * Attempts made without a path before one is requested.
         *
         * **Left at Python's value on purpose.** The gate earns its place because of where this
         * implementation gets the recipient's public key: from the `Destination`, which can be
         * known without a path, so a send with no path is not certainly futile and a try spent on
         * it can still succeed. A design that instead reads the key out of the path entry cannot
         * succeed without one, and there the gate is pure latency — which is why a larger value
         * gets proposed. Raising it to four here would mean four blind sends before we ask where
         * the peer is: a regression in the only place the number is actually read.
         */
        const val MAX_PATHLESS_TRIES = 1

        /**
         * How old an undelivered message may get before it is failed, in milliseconds.
         *
         * **The only terminal bound in the outbound path.** Everything else reschedules. A day is
         * chosen against the case this router exists for: a peer that is simply not switched on.
         * Failing a message because the recipient's phone was in a drawer over lunch is the wrong
         * answer, and it is the answer an attempt-count bound gives.
         */
        const val MAX_OUTBOUND_AGE = 24L * 60 * 60 * 1000

        /**
         * How long after discarding a destination's path before it may be discarded again, in
         * milliseconds.
         *
         * Without this, several queued messages to one absent peer each exhaust their attempts at
         * about the same moment and each throw the path away and ask for a new one, which is a
         * burst of path requests and the answering announces at exactly the moment the interface is
         * least able to carry them.
         */
        const val PATH_INVALIDATION_COOLDOWN = 30000L

        /**
         * Attempts after which a path is suspected, before the full budget is spent.
         *
         * An early, cheap version of what happens at [MAX_DELIVERY_ATTEMPTS]: three failures in a
         * row against a path that exists is already good evidence the path is stale, and asking for
         * a fresh one now means the remaining attempts are spent on a route that might work rather
         * than on the one already known not to.
         */
        const val PATH_DEMOTE_ATTEMPTS = 3

        /**
         * How long a message waits after its route was thrown away, in milliseconds.
         *
         * It is not really a wait — it is a parking spot. A message here is released the moment a
         * path for its destination appears, which is what `wakeMessagesWithFreshPath` does. This is
         * only the backstop for the case where no path ever arrives, and it is long because
         * retrying sooner would just spend attempts on a destination Reticulum has said nothing
         * about.
         */
        const val UNPROVEN_ROUTE_RETRY_WAIT = 300000L

        /** LXMF timestamps are epoch *seconds* as a Double; the queue works in milliseconds. */
        private const val MILLIS_PER_SECOND = 1000

        /**
         * How long a resource transfer may sit in [MessageState.SENDING] before it is sent again.
         *
         * Long enough not to interrupt a slow but working transfer — an image-sized payload can
         * take tens of seconds over a constrained carrier — and short enough that a transfer whose
         * link died is not simply lost.
         */
        private const val SENDING_STALL_TIMEOUT = 120000L

        /**
         * How long a link may sit unestablished before the message waiting on it gives up on it.
         *
         * Generous against Reticulum's own `ESTABLISHMENT_TIMEOUT_PER_HOP` of six seconds, because
         * this is a backstop for the case where the watchdog did not fire at all rather than a
         * second opinion about how long establishment should take.
         */
        private const val LINK_ESTABLISHMENT_TIMEOUT = 30000L

        /** A route reaching the peer with no node in between. See [hasBetterRouteThanLink]. */
        private const val DIRECT_ROUTE_HOPS = 1

        /**
         * The pause between a link reporting ACTIVE and the first packet down it.
         *
         * The far side arms its receive callback when its own link-established callback runs, and
         * that is not synchronous with our proof validating.
         */
        private const val LINK_READY_GRACE = 100L

        /** Message expiry time in milliseconds (30 days) */
        const val MESSAGE_EXPIRY = 30L * 24 * 60 * 60 * 1000

        /** Maximum messages in propagation transfer */
        const val PROPAGATION_LIMIT = 256

        /** Maximum messages in delivery transfer */
        const val DELIVERY_LIMIT = 1000

        /** Maximum peering connections */
        const val MAX_PEERS = 20

        /** Default propagation cost */
        const val PROPAGATION_COST = 16

        /** Strict message validation mode */
        const val STRICT = false

        /** LXMF app name */
        const val APP_NAME = "lxmf"

        /** LXMF delivery aspect */
        const val DELIVERY_ASPECT = "delivery"

        /** LXMF propagation aspect */
        const val PROPAGATION_ASPECT = "propagation"
    }

    // ===== Message Queues =====
    //
    // Lock ordering (to avoid deadlock on any future nested acquisition):
    //   pendingOutboundMutex  →  failedOutboundMutex
    //
    // Today the only nested site is inside [processOutbound] where a
    // FAILED-state message is moved from pendingOutbound to failedOutbound.
    // The reverse order is never acquired. Any new code that needs both
    // mutexes MUST follow this order, or restructure to take them
    // independently.

    /** Pending inbound messages awaiting processing */
    private val pendingInbound = mutableListOf<LXMessage>()
    private val pendingInboundMutex = Mutex()

    /** Pending outbound messages awaiting delivery */
    private val pendingOutbound = mutableListOf<LXMessage>()
    private val pendingOutboundMutex = Mutex()

    /** Failed outbound messages */
    private val failedOutbound = mutableListOf<LXMessage>()
    private val failedOutboundMutex = Mutex()

    // ===== Route Retry State =====
    //
    // Held beside the queue rather than on [LXMessage], because none of it is part of a message:
    // it is what this router has learned about *routes* while trying to deliver. A cooldown belongs
    // to a destination, and the other two are per message but exist only while it is queued.
    //
    // All three are guarded by [retryStateLock]. Every critical section is a few map operations and
    // nothing suspends inside one.

    private val retryStateLock = Any()

    /** When each destination's path was last thrown away, by destination hex hash. */
    private val lastPathInvalidation = mutableMapOf<String, Long>()

    /** How many times in a row a message has been rescheduled for want of a path, by message hex hash. */
    private val pathlessBackoffSteps = mutableMapOf<String, Int>()

    /**
     * Messages parked because their route absorbed a whole attempt budget without proving itself,
     * by message hex hash.
     *
     * Released early by [wakeMessagesWithFreshPath] the moment a path to the destination exists
     * again, which is the difference between a park and a sentence.
     */
    private val awaitingFreshPath = mutableSetOf<String>()

    // ===== Destinations and Links =====

    /** Registered delivery destinations: hash -> Destination */
    private val deliveryDestinations = ConcurrentHashMap<String, DeliveryDestination>()

    /** Active direct links: destination_hash -> Link */
    private val directLinks = ConcurrentHashMap<String, Link>()

    /** Backchannel links for replies: destination_hash -> Link */
    private val backchannelLinks = ConcurrentHashMap<String, Link>()

    /** Destinations with pending link establishments: destination_hash -> started_at_ms */
    private val pendingLinkEstablishments = ConcurrentHashMap<String, Long>()


    /** Pending resource transfers: message_hash -> (message, resource) */
    private val pendingResources = ConcurrentHashMap<String, Pair<LXMessage, Resource>>()

    // ===== Propagation Node Tracking =====

    /** Known propagation nodes: destination_hash -> PropagationNode */
    private val propagationNodes = ConcurrentHashMap<String, PropagationNode>()

    /** Active propagation node for outbound messages */
    @Volatile
    private var activePropagationNodeHash: String? = null

    /** Link to active propagation node */
    @Volatile
    private var outboundPropagationLink: Link? = null

    /** Propagation transfer state */
    @Volatile
    var propagationTransferState: PropagationTransferState = PropagationTransferState.IDLE
        private set

    /** Progress of current propagation transfer (0.0 to 1.0) */
    @Volatile
    var propagationTransferProgress: Double = 0.0
        private set

    /** Number of messages retrieved in last transfer */
    @Volatile
    var propagationTransferLastResult: Int = 0
        private set

    // ===== Configuration =====

    /** Maximum incoming message size in KB. Null = unlimited. */
    var incomingMessageSizeLimitKb: Int? = null

    // ===== Callbacks =====

    /** Callback for delivered messages */
    private var deliveryCallback: ((LXMessage) -> Unit)? = null

    /** Callback for message delivery failures */
    private var failedDeliveryCallback: ((LXMessage) -> Unit)? = null

    // ===== Processing State =====

    /** Whether the router is running */
    @Volatile
    private var running = false

    /**
     * Process-lifetime scope for outbound dispatch + deferred-stamp + cache
     * cleanup work. Previously this was `var processingScope: CoroutineScope?`
     * assigned in [start] and cancelled+nulled in [stop]; [triggerProcessing]
     * and a dozen other call sites used `processingScope.launch { ... }`,
     * which silently no-op'd whenever the scope was null. That created a
     * latent silent-drop hazard: any lifecycle transition that called [stop]
     * without [start] being re-called left `handleOutbound` silently
     * enqueueing messages that no coroutine would ever dispatch — messages
     * "stuck pending" forever in downstream consumer UIs. Reproduced on a
     * Columba device on 2026-04-21 after a Doze cycle.
     *
     * The scope is now owned by the router for its entire object lifetime
     * and is never null. [start] / [stop] gate the PERIODIC processing
     * loop (via the [running] flag and [processingJob]), not the scope
     * itself; one-shot launches from [triggerProcessing] and its peers
     * succeed whether or not [start] has been called. This matches the
     * structural fix pattern from reticulum-kt PR #818
     * (NativeInterfaceFactory) — give the class its own non-null val
     * scope, never let a child-cancellable reference back at it.
     *
     * For non-singleton use (e.g. tests that create many routers, or a
     * rebuild after a reticulum-subprocess restart), call [close] to
     * release the scope — otherwise its [SupervisorJob] anchors any
     * in-flight coroutines until the process exits.
     */
    private val processingScope: CoroutineScope =
        CoroutineScope(Dispatchers.IO + SupervisorJob())

    /** Processing job */
    private var processingJob: Job? = null

    /** Outbound processing mutex */
    private val outboundProcessingMutex = Mutex()

    // ===== Delivery Tracking =====

    /** Locally delivered message transient IDs: hexHash -> timestamp (seconds) */
    private val locallyDeliveredTransientIds = ConcurrentHashMap<String, Long>()

    /** Outbound stamp costs: destination_hash_hex -> [timestamp, cost] */
    private val outboundStampCosts = ConcurrentHashMap<String, Pair<Long, Int>>()

    // ===== Ticket System =====

    /**
     * Available tickets for stamp bypass.
     * Structure mirrors Python's available_tickets dict:
     * - outbound: destHashHex -> Pair(expiresTimestamp, ticketBytes)
     * - inbound: destHashHex -> Map(ticketHex -> expiresTimestamp)
     * - lastDeliveries: destHashHex -> timestamp
     */
    private val outboundTickets = ConcurrentHashMap<String, Pair<Long, ByteArray>>()
    private val inboundTickets = ConcurrentHashMap<String, ConcurrentHashMap<String, Long>>()
    private val lastTicketDeliveries = ConcurrentHashMap<String, Long>()

    // ===== Deferred Stamp Processing =====

    /** Messages awaiting deferred stamp generation: messageIdHex -> LXMessage */
    private val pendingDeferredStamps = ConcurrentHashMap<String, LXMessage>()

    /** Mutex for stamp generation to prevent concurrent CPU-heavy work */
    private val stampGenMutex = Mutex()

    // ===== Access Control =====

    /** Allowed identity hashes (for authentication) */
    private val allowedList = mutableListOf<ByteArray>()

    /** Ignored source destination hashes */
    private val ignoredList = mutableListOf<ByteArray>()

    /** Prioritised destination hashes */
    private val prioritisedList = mutableListOf<ByteArray>()

    /** Whether authentication is required for message delivery */
    private var authRequired: Boolean = false

    // ===== Cleanup Tracking =====

    /** Processing loop counter for scheduling periodic cleanup */
    private var processingCount: Long = 0

    /** File locks for persistence */
    private val costFileMutex = Mutex()
    private val ticketFileMutex = Mutex()
    private val transientIdFileMutex = Mutex()

    // ===== Initialization =====

    init {
        // Create storage directories if needed
        storagePath?.let { path ->
            val dir = File(path, "lxmf")
            if (!dir.exists()) {
                dir.mkdirs()
            }
        }

        // Load persisted state
        loadOutboundStampCosts()
        loadAvailableTickets()
        loadTransientIds()

        // Register announce handlers, mirroring Python LXMRouter.__init__:
        //
        //     RNS.Transport.register_announce_handler(LXMFDeliveryAnnounceHandler(self))
        //     RNS.Transport.register_announce_handler(LXMFPropagationAnnounceHandler(self))
        //
        // Both registrations are required. Without the lxmf.delivery handler,
        // outboundStampCosts never gets populated from peer announces — so
        // handleOutbound emits unstamped wire bytes that any
        // enforce_stamps()-enabled receiver (Sideband with
        // lxmf_require_stamps=True) drops with "Dropping {message} with
        // invalid stamp". Symptom in the wild: Columba sees a delivery proof
        // (RNS-level) but the message never appears at Sideband, only when
        // stamp_cost > 0 is configured on the receiver.

        // lxmf.delivery — Python LXMF/Handlers.py:LXMFDeliveryAnnounceHandler.
        // Transport only calls this handler for announces matching the
        // "lxmf.delivery" destination aspect.
        Transport.registerAnnounceHandler(
            handler =
                AnnounceHandler { destHash, _, appData ->
                    if (appData != null && appData.isNotEmpty()) {
                        handleDeliveryAnnounce(destHash, appData)
                    }
                    false // Don't consume — let other handlers see it too.
                },
            aspectFilter = "lxmf.delivery",
        )

        // lxmf.propagation — Python LXMF/Handlers.py:LXMFPropagationAnnounceHandler.
        Transport.registerAnnounceHandler(
            handler =
                AnnounceHandler { destHash, identity, appData ->
                    if (appData != null && appData.isNotEmpty()) {
                        handlePropagationAnnounce(destHash, identity, appData)
                    }
                    false // Don't consume — let other handlers see it too.
                },
            aspectFilter = "lxmf.propagation",
        )
    }

    /**
     * Data class holding a delivery destination and its configuration.
     */
    data class DeliveryDestination(
        val destination: Destination,
        val identity: Identity,
        val displayName: String? = null,
        var stampCost: Int? = null,
    )

    /**
     * Data class representing a discovered propagation node.
     */
    data class PropagationNode(
        /** The destination hash of the propagation node */
        val destHash: ByteArray,
        /** The identity of the propagation node */
        val identity: Identity,
        /** Display name of the node (from metadata) */
        val displayName: String? = null,
        /** Timebase of the node when announced */
        val timebase: Long = 0,
        /** Whether the node is an active propagation node */
        val isActive: Boolean = true,
        /** Per-transfer limit in KB */
        val perTransferLimit: Int = LXMFConstants.PROPAGATION_LIMIT_KB,
        /** Per-sync limit in KB */
        val perSyncLimit: Int = LXMFConstants.SYNC_LIMIT_KB,
        /** Required stamp cost for message delivery */
        val stampCost: Int = LXMFConstants.PROPAGATION_COST,
        /** Stamp cost flexibility */
        val stampCostFlexibility: Int = LXMFConstants.PROPAGATION_COST_FLEX,
        /** Peering cost */
        val peeringCost: Int = LXMFConstants.PEERING_COST,
        /** When this node was last seen (announcement timestamp) */
        val lastSeen: Long = System.currentTimeMillis(),
    ) {
        val hexHash: String get() = destHash.toHexString()

        override fun equals(other: Any?): Boolean {
            if (this === other) return true
            if (other !is PropagationNode) return false
            return destHash.contentEquals(other.destHash)
        }

        override fun hashCode(): Int = destHash.contentHashCode()
    }

    /**
     * Propagation transfer state for tracking sync progress.
     */
    enum class PropagationTransferState {
        IDLE,
        PATH_REQUESTED,
        LINK_ESTABLISHING,
        LINK_ESTABLISHED,
        LISTING_MESSAGES,
        REQUESTING_MESSAGES,
        RECEIVING_MESSAGES,
        COMPLETE,
        FAILED,
        NO_PATH,
        NO_LINK,
    }

    // ===== Registration Methods =====

    /**
     * Register a delivery identity for receiving LXMF messages.
     *
     * Creates an RNS Destination with the LXMF delivery aspect and sets up
     * the necessary callbacks for message reception.
     *
     * @param identity The identity to register for delivery
     * @param displayName Optional display name for announcements
     * @param stampCost Optional stamp cost requirement
     * @return The created destination
     */
    fun registerDeliveryIdentity(
        identity: Identity,
        displayName: String? = null,
        stampCost: Int? = null,
    ): Destination {
        // Create destination with LXMF delivery aspect
        val destination =
            Destination.create(
                identity = identity,
                direction = DestinationDirection.IN,
                type = DestinationType.SINGLE,
                appName = APP_NAME,
                DELIVERY_ASPECT,
            )

        // Enable ratchets for forward secrecy. Filename matches the Python LXMF
        // reference (`<hash>.ratchets`) so a ratchet directory copied from
        // Sideband / reference-Python installs is readable without a rename
        // step. Legacy LXMF-kt installs wrote the same blob without a suffix;
        // `migrateLegacyRatchetFiles` below handles the one-time rename.
        storagePath?.let { path ->
            val ratchetsDir = File("$path/lxmf/ratchets")
            migrateLegacyRatchetFiles(ratchetsDir, destination.hexHash)
            val ratchetPath = File(ratchetsDir, "${destination.hexHash}.ratchets").absolutePath
            destination.enableRatchets(ratchetPath)
        }

        // Set up packet callback for incoming messages
        destination.packetCallback = { data, packet ->
            handleDeliveryPacket(data, destination, packet as? Packet)
        }

        // Set up link established callback
        destination.setLinkEstablishedCallback { link ->
            handleDeliveryLinkEstablished(link as Link, destination)
        }

        // Set default app data for announcements
        if (displayName != null || (stampCost != null && stampCost > 0)) {
            val appData = packAnnounceAppData(displayName, stampCost ?: 0)
            destination.setDefaultAppData(appData)
        }

        // Register with Transport so it can receive packets
        Transport.registerDestination(destination)

        // Store the delivery destination
        val deliveryDest =
            DeliveryDestination(
                destination = destination,
                identity = identity,
                displayName = displayName,
                stampCost = stampCost,
            )
        deliveryDestinations[destination.hexHash] = deliveryDest

        return destination
    }

    /**
     * Register a callback for delivered messages.
     *
     * The callback will be invoked when a message is successfully received
     * and validated.
     *
     * @param callback Function to call with delivered messages
     */
    fun registerDeliveryCallback(callback: (LXMessage) -> Unit) {
        deliveryCallback = callback
    }

    /**
     * Register a callback for failed message deliveries.
     *
     * @param callback Function to call when message delivery fails
     */
    fun registerFailedDeliveryCallback(callback: (LXMessage) -> Unit) {
        failedDeliveryCallback = callback
    }

    // ===== Outbound Message Handling =====

    /**
     * Handle an outbound message by queuing it for delivery.
     *
     * This method prepares the message for sending and adds it to the
     * outbound queue for processing.
     *
     * @param message The message to send
     */
    suspend fun handleOutbound(message: LXMessage) {
        val destHashHex = message.destinationHash.toHexString()

        // Auto-configure stamp cost from outbound_stamp_costs if not set
        if (message.stampCost == null) {
            val costEntry = outboundStampCosts[destHashHex]
            if (costEntry != null) {
                message.stampCost = costEntry.second
                RnsLog.debug("LXMRouter") { "Auto-configured stamp cost to ${costEntry.second} for $destHashHex" }
            }
        }

        // Set message state to outbound
        message.state = MessageState.OUTBOUND

        // Attach outbound ticket if available
        message.outboundTicket = getOutboundTicket(destHashHex)
        if (message.outboundTicket != null && message.deferStamp) {
            // Ticket bypass means no PoW needed — don't defer
            message.deferStamp = false
        }

        // Include ticket for reply if requested
        if (message.includeTicket) {
            val ticket = generateTicket(message.destinationHash)
            if (ticket != null) {
                message.fields[LXMFConstants.FIELD_TICKET] = ticket
            }
        }

        // Pack the message if not already packed
        if (message.packed == null) {
            message.pack()
        }

        // If stamp is deferred and no cost, don't defer
        if (message.deferStamp && message.stampCost == null) {
            message.deferStamp = false
        }

        if (!message.deferStamp) {
            // Generate stamp now if needed (before queueing)
            if (message.stampCost != null && message.stamp == null && message.outboundTicket == null) {
                message.getStamp()
                // Re-pack with stamp
                message.repackWithStamp()
            }

            // Add to outbound queue
            pendingOutboundMutex.withLock {
                pendingOutbound.add(message)
            }

            // Trigger processing
            triggerProcessing()
        } else {
            // Deferred: stamp will be generated in background
            val messageIdHex = message.hash?.toHexString() ?: return
            pendingDeferredStamps[messageIdHex] = message
        }
    }

    /**
     * Trigger outbound processing.
     */
    private fun triggerProcessing() {
        processingScope.launch {
            processOutbound()
        }
    }

    /**
     * Process pending outbound messages.
     *
     * This is the main delivery loop that handles different delivery methods.
     */
    suspend fun processOutbound() {
        if (!outboundProcessingMutex.tryLock()) {
            return // Already processing
        }

        try {
            val toRemove = mutableListOf<LXMessage>()
            val toDispatch = mutableListOf<LXMessage>()
            val toFailCallback = mutableListOf<LXMessage>()
            val currentTime = System.currentTimeMillis()

            // Classify current pendingOutbound under the queue lock. Do NOT
            // invoke user callbacks or per-message delivery dispatch here —
            // those can suspend on network I/O (packet.send over a flaky
            // interface) and would wedge every concurrent handleOutbound
            // caller on this same mutex. See LXMRouterTest
            // "handleOutbound not blocked when processOutboundMessage hangs".
            pendingOutboundMutex.withLock {
                wakeMessagesWithFreshPath(pendingOutbound)

                if (pendingOutbound.isNotEmpty()) {
                    RnsLog.debug("LXMRouter") {
                        "QUEUE census: " + pendingOutbound.joinToString { m ->
                            "${m.destinationHash.toHexString().take(8)}" +
                                " state=${m.state} method=${m.desiredMethod}" +
                                " attempts=${m.deliveryAttempts}" +
                                " dueIn=${(m.nextDeliveryAttempt ?: 0L) - currentTime}ms"
                        }
                    }
                }

                for (message in pendingOutbound) {
                    if (message.hasOutlivedTheQueue(currentTime)) {
                        RnsLog.warn("LXMRouter") { "Giving up on a message to ${message.destinationHash.toHexString()} after $MAX_OUTBOUND_AGE ms" }

                        message.state = MessageState.FAILED
                    }

                    when (message.state) {
                        MessageState.DELIVERED -> {
                            toRemove.add(message)
                        }

                        MessageState.SENT -> {
                            // For propagated messages, SENT is final
                            if (message.method == DeliveryMethod.PROPAGATED) {
                                toRemove.add(message)
                            } else if (currentTime >= (message.nextDeliveryAttempt ?: 0L)) {
                                // **Sent is not delivered.** An opportunistic message — and a
                                // direct one small enough to travel as a packet — reaches SENT the
                                // moment it is handed to the transport, and only a delivery proof
                                // can move it past that. Without this branch nothing ever looks at
                                // it again: it is not OUTBOUND so it is never dispatched, and it is
                                // not terminal so it is never removed. The symptom is a queue of
                                // messages stuck at SENT and overdue by minutes with the recipient
                                // one hop away: the send went into a write that was lost, and
                                // nothing retried it.
                                //
                                // So a message whose retry wait has passed with no proof goes back
                                // to OUTBOUND and is sent again, earning a fresh delivery proof
                                // each time, until a proof arrives or the route budget is spent.
                                // Python keeps a DIRECT message in the queue until DELIVERED for
                                // the same reason.
                                //
                                // Re-sending can deliver the same message twice; the receiving side
                                // drops the duplicate on its transient id, which is what that cache
                                // is for.
                                message.state = MessageState.OUTBOUND

                                // Spread on the same curve the pathless retries use, rather than a
                                // flat wait. Flat, every unproven message re-sends every ten
                                // seconds until its budget is gone — and a command fanned out to
                                // every contact turns a handful of absent peers into several times
                                // as many transmissions on whatever carrier is narrowest, with
                                // ordinary messages timing out behind them.
                                message.nextDeliveryAttempt = currentTime +
                                    maxOf(DELIVERY_RETRY_WAIT, RetryBackoff.afterStep(message.deliveryAttempts))

                                toDispatch.add(message)
                            }
                        }

                        MessageState.CANCELLED, MessageState.REJECTED -> {
                            toRemove.add(message)
                            toFailCallback.add(message)
                        }

                        MessageState.FAILED -> {
                            toRemove.add(message)
                            failedOutboundMutex.withLock {
                                failedOutbound.add(message)
                            }
                            toFailCallback.add(message)
                        }

                        MessageState.OUTBOUND -> {
                            // Check if ready to attempt delivery. The actual
                            // dispatch happens AFTER we release the lock.
                            val nextAttempt = message.nextDeliveryAttempt ?: 0L
                            if (currentTime >= nextAttempt) {
                                toDispatch.add(message)
                            }
                        }

                        MessageState.SENDING -> {
                            // A resource transfer in flight, which is the right place to be — until
                            // it is not. The transfer reports completion and failure through
                            // callbacks, so a link that dies without either leaves the message
                            // here with nothing to move it: not OUTBOUND, so never dispatched, and
                            // not terminal, so never removed. The symptom is an entry sitting in
                            // SENDING well past its due time with nothing ever looking at it.
                            //
                            // The bound is deliberately generous. A real transfer takes as long as
                            // the carrier needs, and tens of kilobytes over a constrained one takes
                            // tens of seconds — so this is a wedge-breaker, not an opinion about
                            // how fast a resource ought to be.
                            if (currentTime - (message.nextDeliveryAttempt ?: currentTime) > SENDING_STALL_TIMEOUT) {
                                RnsLog.warn("LXMRouter") { "A resource to ${message.destinationHash.toHexString()} stalled; sending it again" }

                                message.state = MessageState.OUTBOUND

                                toDispatch.add(message)
                            }
                        }

                        else -> {
                            // Other states (GENERATING) - wait
                        }
                    }
                }

                // Remove processed messages
                pendingOutbound.removeAll(toRemove)
            }

            // Dispatch per-message delivery OUTSIDE the queue lock. This is
            // where the real work (and any blocking I/O) happens. Holding the
            // queue mutex across these calls — as this method used to — would
            // wedge every handleOutbound caller behind whatever interface
            // blocks longest.
            //
            // Intra-batch ordering is intentionally sequential: messages
            // snapshotted into toDispatch in one processOutbound() call are
            // dispatched in FIFO order, one after the other, under the
            // outboundProcessingMutex guard. This means a slow send for the
            // first message still delays the second message in the same
            // batch — but this no longer blocks unrelated handleOutbound
            // callers (which was the bug this PR fixes). Parallelising
            // intra-batch dispatch (e.g. launching per-message coroutines)
            // is a possible future optimisation; it is deliberately not done
            // here to keep the fix minimal and preserve the existing
            // delivery-ordering contract.
            for (message in toDispatch) {
                processOutboundMessage(message)
            }

            // User callbacks for terminal-state messages also run outside
            // the lock — same rationale. A slow or misbehaving callback
            // shouldn't block unrelated sends from being queued.
            val failedCb = failedDeliveryCallback
            if (failedCb != null) {
                for (message in toFailCallback) {
                    failedCb.invoke(message)
                }
            }
        } finally {
            outboundProcessingMutex.unlock()
        }
    }

    /**
     * Test-only hook fired at the start of [processOutboundMessage]. Exists
     * so a regression test for "mutex held across per-message dispatch" can
     * simulate a slow / hung delivery by suspending in this hook; any
     * coroutine trying to take [pendingOutboundMutex] concurrently reveals
     * whether the mutex is still held while the hook is in-flight. Production
     * code MUST NOT install this.
     */
    @org.jetbrains.annotations.VisibleForTesting
    internal var testHookOnProcessOutboundMessage: (suspend (LXMessage) -> Unit)? = null

    /** Test seam: the current DIRECT delivery link for a destination, if any. */
    @org.jetbrains.annotations.VisibleForTesting
    internal fun directLinkForTest(destHashHex: String): Link? = directLinks[destHashHex]

    /**
     * Process a single outbound message based on its delivery method.
     */
    private suspend fun processOutboundMessage(message: LXMessage) {
        testHookOnProcessOutboundMessage?.invoke(message)

        val method = message.desiredMethod ?: DeliveryMethod.DIRECT

        when (method) {
            DeliveryMethod.OPPORTUNISTIC -> {
                processOpportunisticDelivery(message)
            }

            DeliveryMethod.DIRECT -> {
                processDirectDelivery(message)
            }

            DeliveryMethod.PROPAGATED -> {
                processPropagatedDelivery(message)
            }

            DeliveryMethod.PAPER -> {
                // Paper delivery is handled separately (QR codes, etc.)
                message.state = MessageState.FAILED
            }
        }
    }

    /**
     * Throws away the known path to [destinationHash] and asks for a fresh one, unless the same
     * destination was cleared too recently.
     *
     * The lighter alternative — marking the path unresponsive and keeping the entry — does not work
     * on a mesh where a transport node is still cheerfully announcing the stale route: it simply
     * re-asserts it, and every retry falls into the same hole while the peer sits one hop away on a
     * carrier we are not using. Removing the entry outright makes the next announce win the path on
     * merit, and the nearer carrier usually does.
     *
     * The cooldown is what stops that from becoming a loop of its own. Clearing a path provokes an
     * announce that re-learns it; if the re-learned route is the same stale one we would clear it
     * again a few attempts later, and several messages queued for one absent peer each do this at
     * once. The gap gives a re-learned route a real chance to be tried before it is written off.
     *
     * @return true if the path was cleared, false if the cooldown suppressed it.
     */
    private fun invalidateUnprovenPath(destinationHash: ByteArray): Boolean {
        val key = destinationHash.toHexString()
        val now = System.currentTimeMillis()

        synchronized(retryStateLock) {
            val last = lastPathInvalidation[key]

            if (last != null && now - last < PATH_INVALIDATION_COOLDOWN) return false

            lastPathInvalidation[key] = now
        }

        Transport.expirePath(destinationHash)
        Transport.requestPath(destinationHash)

        RnsLog.info("LXMRouter") { "Cleared unproven path to $key and requested a fresh one" }

        return true
    }

    /**
     * Reschedules a message that could not be attempted because there is no path, spending no part
     * of its delivery budget.
     *
     * [MAX_DELIVERY_ATTEMPTS] counts *transmissions* — sends that were made and went unproven.
     * Charging it for "this device had no route" is what made the age bound unreachable in exactly
     * the case it exists for: a message with no path exhausted the budget in a couple of minutes
     * and was discarded while the radio it was waiting for was still coming back up.
     *
     * No attempt is refunded here, because there is nothing to refund: each branch bills its own
     * attempt and the pathless branches simply do not. The alternative shape — incrementing once
     * at the top of the loop and having every branch undo it — is what needs a refund; this one
     * does not.
     */
    private fun rescheduleWithoutPath(message: LXMessage) {
        val step = message.hash?.let { hash ->
            val key = hash.toHexString()

            synchronized(retryStateLock) {
                val current = pathlessBackoffSteps[key] ?: 0
                pathlessBackoffSteps[key] = current + 1

                current
            }
        } ?: 0

        message.nextDeliveryAttempt = System.currentTimeMillis() + RetryBackoff.afterStep(step)
    }

    /**
     * Handles a message whose route has absorbed [MAX_DELIVERY_ATTEMPTS] sends without a single
     * delivery proof.
     *
     * **This is not a failure.** Eight unproven sends says the route is not delivering, not that
     * the message is undeliverable, and the two are routinely confused by a stale multi-hop entry
     * pointing away from a peer that is one hop away on another carrier. So the path goes, the
     * budget resets for whatever route replaces it, and the message waits.
     *
     * The wait is a ceiling rather than a sentence: [wakeMessagesWithFreshPath] releases it as soon
     * as a path exists again. Only [MAX_OUTBOUND_AGE] ever gives up.
     */
    private fun parkOnExhaustedRoute(message: LXMessage) {
        invalidateUnprovenPath(message.destinationHash)

        message.deliveryAttempts = 0
        message.state = MessageState.OUTBOUND
        message.nextDeliveryAttempt = System.currentTimeMillis() + UNPROVEN_ROUTE_RETRY_WAIT

        message.hash?.toHexString()?.let { key ->
            synchronized(retryStateLock) {
                // The replacement route deserves a clean slate rather than inheriting the dead
                // one's climb.
                pathlessBackoffSteps[key] = 0
                awaitingFreshPath.add(key)
            }
        }

        RnsLog.warn("LXMRouter") {
            "Route to ${message.destinationHash.toHexString()} took $MAX_DELIVERY_ATTEMPTS unproven sends; cleared it and parked the message"
        }
    }

    /**
     * Releases every parked or backed-off message whose destination is reachable again, and forgets
     * the retry state of messages that have left the queue.
     *
     * Without this a message that ran out of route sits out the full [UNPROVEN_ROUTE_RETRY_WAIT]
     * even though the peer walked back into range ten seconds later — a timer already overtaken by
     * events. The backed-off ones matter just as much: a message five steps up the curve is waiting
     * over a minute for a link that is now up.
     *
     * @param queue the outbound queue, which the caller must already hold the lock on.
     */
    private fun wakeMessagesWithFreshPath(queue: List<LXMessage>) {
        val live = queue.mapNotNull { it.hash?.toHexString() }.toSet()

        synchronized(retryStateLock) {
            awaitingFreshPath.retainAll(live)
            pathlessBackoffSteps.keys.retainAll(live)
        }

        val waiting = queue.filter { isWaitingOnAPath(it) }

        if (waiting.isEmpty()) return

        // One path lookup per destination rather than per message: several messages to the same
        // absent peer is the ordinary case, not the exotic one.
        val reachable = waiting
            .distinctBy { it.destinationHash.toHexString() }
            .filter { Transport.hasPath(it.destinationHash) }
            .map { it.destinationHash.toHexString() }
            .toSet()

        val now = System.currentTimeMillis()

        waiting.filter { it.destinationHash.toHexString() in reachable }.forEach { message ->
            message.nextDeliveryAttempt = now

            message.hash?.toHexString()?.let { key ->
                synchronized(retryStateLock) {
                    awaitingFreshPath.remove(key)
                    // The outage is over, so a later failure starts the curve again rather than
                    // resuming a climb that belonged to it.
                    pathlessBackoffSteps[key] = 0
                }
            }

            RnsLog.debug("LXMRouter") {
                "Path to ${message.destinationHash.toHexString()} is back; releasing a waiting message early"
            }
        }
    }

    /**
     * Whether this message has been queued longer than [MAX_OUTBOUND_AGE] and should be given up on.
     *
     * The single terminal bound in the outbound path, now that spending a delivery budget only
     * costs the message its route. A message with no timestamp is never expired by age: it has no
     * age to judge, and guessing one would fail messages for a missing field.
     */
    private fun LXMessage.hasOutlivedTheQueue(now: Long): Boolean {
        if (state == MessageState.DELIVERED || state == MessageState.FAILED) return false

        val createdAt = timestamp ?: return false

        return now - (createdAt * MILLIS_PER_SECOND).toLong() > MAX_OUTBOUND_AGE
    }

    /**
     * Puts a message that failed *this attempt* back on the queue instead of ending it.
     *
     * A receipt that times out and a resource transfer that gives up both mean the same thing: this
     * try did not prove out. Neither means the message cannot be delivered — the receipt budget is
     * derived from the hop count and a slow Bluetooth link beats it routinely, and a resource dies
     * whenever the link under it does. Ending the message there contradicts the whole retry model,
     * which says [MAX_OUTBOUND_AGE] is the only terminal bound: a long message goes `SENDING` →
     * `FAILED` the moment its resource concludes empty, and nothing tries again.
     *
     * The failed callback still fires, because callers want to know an attempt went unproven; what
     * changes is that the message stays in the queue and is sent again.
     */
    private fun retryAfterUnproven(message: LXMessage, reason: String) {
        RnsLog.debug("LXMRouter") { "$reason for ${message.destinationHash.toHexString()}; will try again" }

        message.state = MessageState.OUTBOUND
        message.nextDeliveryAttempt = System.currentTimeMillis() +
            maxOf(DELIVERY_RETRY_WAIT, RetryBackoff.afterStep(message.deliveryAttempts))

        message.failedCallback?.invoke(message)
    }

    /** Whether [message] is being held back by the absence of a route rather than by its own schedule. */
    private fun isWaitingOnAPath(message: LXMessage): Boolean {
        val key = message.hash?.toHexString() ?: return false

        return synchronized(retryStateLock) {
            key in awaitingFreshPath || (pathlessBackoffSteps[key] ?: 0) > 0
        }
    }

    /**
     * Process opportunistic message delivery.
     *
     * Matches Python LXMF LXMRouter opportunistic outbound handling (lines 2554-2581):
     * - After MAX_PATHLESS_TRIES attempts without path, request path
     * - At MAX_PATHLESS_TRIES+1 with path but still failing, rediscover path
     * - Normal delivery attempt otherwise
     */
    private suspend fun processOpportunisticDelivery(message: LXMessage) {
        // The route's budget, not the message's life. Spending it clears the path and parks the
        // message rather than failing it — see [parkOnExhaustedRoute].
        if (message.deliveryAttempts >= MAX_DELIVERY_ATTEMPTS) {
            parkOnExhaustedRoute(message)

            return
        }

        val dest = message.destination
        if (dest == null) {
            // No destination, can't check path - schedule retry
            message.nextDeliveryAttempt = System.currentTimeMillis() + DELIVERY_RETRY_WAIT
            return
        }

        // Failover assist, well before the budget runs out: three unproven sends against a path
        // that exists is already good evidence the path is the problem, and the attempts left are
        // better spent on a route that might work than on the one already known not to. The
        // cooldown inside [invalidateUnprovenPath] keeps this from becoming a loop.
        if (message.deliveryAttempts == PATH_DEMOTE_ATTEMPTS && Transport.hasPath(dest.hash)) {
            invalidateUnprovenPath(dest.hash)
        }

        val hasPath = Transport.hasPath(dest.hash)

        when {
            // After MAX_PATHLESS_TRIES attempts without path, request path
            // Python: if delivery_attempts >= MAX_PATHLESS_TRIES and not has_path()
            message.deliveryAttempts >= MAX_PATHLESS_TRIES && !hasPath -> {
                RnsLog.debug("LXMRouter") { "Requesting path after ${message.deliveryAttempts} pathless tries for ${message.destinationHash.toHexString()}" }

                Transport.requestPath(dest.hash)

                // Nothing was transmitted, so nothing is billed and the wait comes off the backoff
                // curve rather than being a flat PATH_REQUEST_WAIT. A coverage blip of a couple of
                // seconds is then recovered in a couple of seconds, and a peer that has been dark
                // for an hour costs almost nothing.
                rescheduleWithoutPath(message)

                message.progress = 0.01
            }

            // Normal delivery attempt
            // Python: else: if time.time() > next_delivery_attempt
            else -> {
                val now = System.currentTimeMillis()
                val nextAttempt = message.nextDeliveryAttempt ?: 0L
                if (nextAttempt == 0L || now > nextAttempt) {
                    message.deliveryAttempts++
                    message.nextDeliveryAttempt = now + DELIVERY_RETRY_WAIT
                    RnsLog.debug("LXMRouter") { "Opportunistic delivery attempt ${message.deliveryAttempts} for ${message.destinationHash.toHexString()}" }

                    val sent = sendOpportunisticMessage(message)
                    if (sent) {
                        message.state = MessageState.SENT
                        message.method = DeliveryMethod.OPPORTUNISTIC
                    }
                }
            }
        }
    }

    /**
     * Send a message opportunistically (single encrypted packet).
     *
     * This uses Packet.create() with the destination object, which matches
     * how Python RNS creates packets. The Packet class handles encryption
     * automatically based on destination type.
     */
    private fun sendOpportunisticMessage(message: LXMessage): Boolean {
        // Get the packed message data
        val packed = message.packed ?: return false

        // Get the destination - required for encryption
        val dest = message.destination
        if (dest == null) {
            RnsLog.error("LXMRouter") { "Cannot send opportunistic: no destination" }
            return false
        }

        // For opportunistic delivery, we send:
        // - Destination hash is in the packet header
        // - Data is: source_hash + signature + payload (everything after dest hash)
        // - This data is encrypted by Packet.create() for the destination
        val plainData = packed.copyOfRange(LXMFConstants.DESTINATION_LENGTH, packed.size)

        // Debug logging
        val destPubKey = dest.identity?.getPublicKey()
        RnsLog.debug("LXMRouter") { "sendOpportunisticMessage:" }
        RnsLog.debug("LXMRouter") { "Destination hash: ${message.destinationHash.toHexString()}" }
        RnsLog.debug("LXMRouter") { "Dest identity hash: ${dest.identity?.hash?.toHexString() ?: "null"}" }
        RnsLog.debug("LXMRouter") { "Dest public key (first 8 bytes): ${destPubKey?.take(8)?.toByteArray()?.toHexString() ?: "null"}" }
        RnsLog.debug("LXMRouter") { "Plain data size: ${plainData.size} bytes" }
        RnsLog.debug("LXMRouter") { "Plain data (first 32 bytes): ${plainData.take(32).toByteArray().toHexString()}" }

        // Create the packet - Packet.create() will encrypt the data for us
        val packet =
            Packet.create(
                destination = dest,
                data = plainData,
                packetType = PacketType.DATA,
                context = PacketContext.NONE,
                transportType = TransportType.BROADCAST,
            )

        RnsLog.debug("LXMRouter") { "Packed packet size: ${packet.raw?.size ?: packet.pack().size} bytes" }

        // Send via packet.send() to get receipt for delivery confirmation
        val receipt = packet.send()
        if (receipt != null) {
            RnsLog.debug("LXMRouter") { "Sent opportunistic message to ${message.destinationHash.toHexString()}" }

            // Dual-dispatch, and this is the moment for it. The route this packet has just gone
            // out by may be a multi-hop path that cannot actually deliver — a peer standing
            // next to this device reached through somebody else's relay — while the direct carrier
            // link to it sits idle. Sending the same packet over a carrier the peer was recently
            // heard on gets it there without waiting for several unproven attempts to prove the
            // route dead. The receiver drops whichever copy arrives second on its packet hash, so
            // this is free wherever it was not needed. Best effort: a failure here changes nothing
            // about the send that already succeeded.
            packet.raw?.let { raw ->
                runCatching { Transport.sendFallbackCopy(message.destinationHash, raw) }
            }

            // Set up delivery confirmation callback
            receipt.setDeliveryCallback { _ ->
                message.state = MessageState.DELIVERED
                message.deliveryCallback?.invoke(message)
            }

            // Set up timeout callback
            receipt.setTimeoutCallback { _ ->
                retryAfterUnproven(message, "An opportunistic send went unproven")
            }

            return true
        } else {
            RnsLog.error("LXMRouter") { "Failed to send opportunistic message" }
            return false
        }
    }

    /**
     * Process direct link-based message delivery.
     *
     * Matches Python LXMF LXMRouter.py lines 2578-2655:
     *  - PENDING link → wait passively (do not retry, do not increment attempts).
     *    reticulum-kt's Link watchdog runs its own establishment timeout
     *    (ESTABLISHMENT_TIMEOUT_PER_HOP × hops). When that fires, the link's
     *    closedCallback removes the entry from `directLinks` and the next tick
     *    creates a fresh attempt.
     *  - CLOSED link → clear the dead entry and schedule a retry.
     *  - No link → bump `deliveryAttempts` and attempt to establish a new one.
     *
     * Previous behaviour incremented `deliveryAttempts` unconditionally on
     * every entry, eagerly tore down PENDING links after 15s (racing the Link
     * watchdog and resetting the establishment clock), and—critically—failed
     * messages without invoking `failedCallback`, which silently swallowed the
     * "DIRECT failed → fall back to PROPAGATED" hook in upstream callers.
     */
    private suspend fun processDirectDelivery(message: LXMessage) {
        // The route's budget, not the message's life — as in [processOpportunisticDelivery].
        if (message.deliveryAttempts >= MAX_DELIVERY_ATTEMPTS) {
            parkOnExhaustedRoute(message)

            return
        }

        // Failover assist, as in [processOpportunisticDelivery]: three unproven sends against a
        // path that exists is evidence the path is the problem, and clearing it lets the next
        // announce win the route on merit rather than re-asserting the one already failing.
        if (message.deliveryAttempts == PATH_DEMOTE_ATTEMPTS && Transport.hasPath(message.destinationHash)) {
            invalidateUnprovenPath(message.destinationHash)
        }

        val destHashHex = message.destinationHash.toHexString()

        // Check for existing active link — prefer directLinks (WE initiated),
        // fall back to backchannelLinks (THEY initiated). Python LXMF sets up
        // resource receive callbacks on ALL links via delivery_link_established,
        // so backchannel links are safe to send on.
        var link = directLinks[destHashHex]
        if (link == null || link.status != LinkConstants.ACTIVE) {
            // Try backchannel if direct link isn't active
            val backchannel = backchannelLinks[destHashHex]
            if (backchannel != null && backchannel.status == LinkConstants.ACTIVE) {
                link = backchannel
            }
        }

        // A held link does not follow the routing table, so a better carrier appearing after it
        // was opened changes nothing for it — see [hasBetterRouteThanLink].
        if (link != null && link.status == LinkConstants.ACTIVE && hasBetterRouteThanLink(link, message.destinationHash)) {
            RnsLog.debug("LXMRouter") { "A better route to $destHashHex exists; replacing its link" }

            // **Stop using it; do not tear it down.** A link carries the delivery proofs for
            // whatever has already gone out on it, and closing it throws those away: the peer has
            // the message and answers it, while the sender sits on a row that still says it never
            // arrived. Observed immediately on doing exactly that — the far phone showed the
            // message received *and* its own reply delivered, and this one still showed the
            // original as unsent.
            //
            // Forgetting it here is enough for the purpose: the next message establishes a fresh
            // link over the better interface, and this one closes on its own timeout once its
            // outstanding proofs have had their chance.
            directLinks.remove(destHashHex)
            pendingLinkEstablishments.remove(destHashHex)

            link = null
        }

        when {
            link != null && link.status == LinkConstants.ACTIVE -> {
                // Use existing active link (direct or backchannel)
                sendViaLink(message, link)
            }

            link != null && link.status == LinkConstants.PENDING -> {
                // Rare, and only a floor. A message that asked for a link is normally released by
                // [wakeMessagesWithActiveLink] the instant it establishes, so it is not still
                // sitting here waiting for a tick to notice. What remains is the case that floor
                // was built for: a link that never establishes at all, whose message would
                // otherwise be examined every tick forever, PENDING being neither dispatched nor
                // terminal — so without this floor such an entry stays overdue indefinitely.
                val startedAt = pendingLinkEstablishments[destHashHex]

                if (startedAt != null && System.currentTimeMillis() - startedAt > LINK_ESTABLISHMENT_TIMEOUT) {
                    RnsLog.warn("LXMRouter") { "Link to $destHashHex never activated; abandoning it and retrying" }

                    directLinks.remove(destHashHex)
                    pendingLinkEstablishments.remove(destHashHex)

                    rescheduleWithoutPath(message)
                }
            }

            link != null && link.status == LinkConstants.CLOSED -> {
                // Safety net only. In kt the per-link [closedCallback] in
                // [establishLinkForMessage] fires on close and eagerly removes the
                // link from directLinks (+ re-requests the path — see there), so a
                // CLOSED link is almost never observed here; the next tick lands in
                // the no-link branch. Python's per-message CLOSED branch
                // (LXMRouter.py:2610-2629) is relocated to that callback because
                // kt's link lifecycle is event-driven (see port-deviations.md).
                // If we do see a CLOSED link (close-callback race), just clear both
                // link maps and reschedule.
                directLinks.remove(destHashHex)
                backchannelLinks.remove(destHashHex)
                pendingLinkEstablishments.remove(destHashHex)
                message.nextDeliveryAttempt = System.currentTimeMillis() + DELIVERY_RETRY_WAIT
            }

            else -> {
                // No link exists. Try to establish one, but only if we've never
                // tried before or the retry-wait window has elapsed. Port of
                // Python LXMRouter.py:2633-2652.
                val now = System.currentTimeMillis()
                val nextAttempt = message.nextDeliveryAttempt ?: 0L
                if (nextAttempt == 0L || now >= nextAttempt) {
                    if (Transport.hasPath(message.destinationHash)) {
                        // Path known — establish the link. (:2642-2647)
                        message.deliveryAttempts++
                        message.nextDeliveryAttempt = now + DELIVERY_RETRY_WAIT

                        establishLinkForMessage(message)

                        message.progress = 0.03
                    } else {
                        // No path — request one. This is the core columba#1004 fix: DIRECT
                        // delivery requests a path instead of blindly attempting a link against a
                        // missing or stale one. (:2648-2652)
                        //
                        // The attempt is no longer billed here, and the increment has moved into
                        // the branch above to make that so: no link was attempted, so this is not
                        // one of the transmissions the budget counts.
                        Transport.requestPath(message.destinationHash)

                        rescheduleWithoutPath(message)

                        message.progress = 0.01
                    }
                }
            }
        }
    }

    /**
     * Whether the routing table now offers something better than the interface this link is stuck on.
     *
     * **A link is bound to one interface for life.** `attachedInterfaceHash` is fixed when it
     * activates, and every send on it goes out that way, bypassing the path table — which is right
     * for a link and wrong for a router that has been holding one since before a better carrier
     * existed. And the ordering makes that the *normal* case for a carrier that has to negotiate
     * itself into being: the negotiation needs a route, the only route is a relay, and establishing
     * it opens the very link that then refuses to move once the direct carrier is up.
     *
     * So a link established over a relay a fraction of a second before the direct route appears
     * goes on sending through that relay indefinitely, while every routed packet in both directions
     * has long since switched to the direct carrier. Only large messages take a link, so it
     * surfaces as short and medium messages going direct while long ones always take the relay.
     *
     * Deliberately narrow. It says yes only when the best route is **direct** — one hop — and runs
     * over a different interface than the link. A direct route is unambiguously better than a link
     * through a relay, and requiring it keeps an oscillating multi-hop path from tearing links down
     * repeatedly, which would cost more than the worse route does.
     */
    private fun hasBetterRouteThanLink(link: Link, destinationHash: ByteArray): Boolean {
        val linkInterface = link.attachedInterfaceHash ?: return false
        val pathInterface = Transport.pathInterfaceHash(destinationHash) ?: return false

        if (linkInterface.contentEquals(pathInterface)) return false

        return Transport.hopsTo(destinationHash) == DIRECT_ROUTE_HOPS
    }

    /**
     * Releases the messages that were waiting for this link, the moment it establishes.
     *
     * **Because the backoff outlives what it was waiting for.** A message that needs a link is
     * tried once, finds none, asks for one, and is rescheduled on the retry curve. The link then
     * comes up in a fraction of a second — and nothing tells the message. The link can be
     * established and carrying resources in both directions while the one message that asked for
     * it still sits on a multi-second backoff, and the exchange it belonged to finishes without
     * it. Smaller messages in the same exchange travel opportunistically and arrive, so the loss
     * is silent and looks like one message going missing rather than a scheduling fault.
     *
     * A router that blocks inside the send, polling the link until it is usable, needs nothing
     * like this. Ours does not work that way: it queues and runs on a tick, so reaching the same
     * place means being *told* rather than asking, and [Link.create]'s established callback is
     * where that happens.
     *
     * The messages are made due rather than sent from here, so a send still happens on the
     * processing loop with every check it makes, one tick later at the outside.
     */
    private fun wakeMessagesWithActiveLink(destHashHex: String) {
        processingScope.launch {
            val now = System.currentTimeMillis()

            val released = pendingOutboundMutex.withLock {
                pendingOutbound
                    .filter { it.destinationHash.toHexString() == destHashHex }
                    .onEach { it.nextDeliveryAttempt = now }
                    .size
            }

            if (released == 0) return@launch

            RnsLog.debug("LXMRouter") {
                "Link to $destHashHex is up; releasing $released waiting message(s) early"
            }
        }
    }

    /**
     * Establish a link for sending a message.
     */
    private fun establishLinkForMessage(message: LXMessage) {
        val destination = message.destination
        if (destination == null) {
            // Can't establish link without destination object
            // For now, schedule retry and hope destination becomes available
            message.nextDeliveryAttempt = System.currentTimeMillis() + PATH_REQUEST_WAIT
            return
        }

        val destHashHex = message.destinationHash.toHexString()

        // Check if we're already establishing a link
        if (pendingLinkEstablishments.containsKey(destHashHex)) {
            message.nextDeliveryAttempt = System.currentTimeMillis() + DELIVERY_RETRY_WAIT
            return
        }

        pendingLinkEstablishments[destHashHex] = System.currentTimeMillis()

        try {
            // Create the link
            val link =
                Link.create(
                    destination = destination,
                    establishedCallback = { establishedLink ->
                        // Link established successfully
                        directLinks[destHashHex] = establishedLink
                        pendingLinkEstablishments.remove(destHashHex)

                        // Set up link callbacks for receiving
                        setupLinkCallbacks(establishedLink, destHashHex)

                        wakeMessagesWithActiveLink(destHashHex)

                        // Identify ourselves on the link
                        identifyOnLink(establishedLink)

                        // Trigger processing to send pending messages
                        triggerProcessing()
                    },
                    closedCallback = { closedLink ->
                        // Re-request the path when a DIRECT delivery link closes.
                        // Port of Python's CLOSED-branch logic (LXMRouter.py:2610-2622):
                        // Python runs this per-message in process_outbound, but kt's
                        // link lifecycle is event-driven and removes the link from
                        // directLinks here (so processDirectDelivery's CLOSED branch is
                        // pre-empted), so the re-request is replicated at the close
                        // event. See port-deviations.md. Without this, transport-enabled
                        // nodes — where reticulum-kt's Transport.deregisterLink path
                        // recovery is intentionally gated off (Python parity) — had no
                        // way to refresh a stale path after a failed link (columba#1004).
                        //
                        // Gate on the message still needing delivery so a normal
                        // post-delivery close doesn't emit a spurious request (Python's
                        // CLOSED branch only runs for messages still in the outbound loop).
                        val stillDelivering = message.state == MessageState.OUTBOUND ||
                            message.state == MessageState.SENDING
                        if (stillDelivering) {
                            if (closedLink.activatedAt > 0) {
                                // Was active, closed unexpectedly — refresh the path. (:2611-2613)
                                Transport.requestPath(message.destinationHash)
                            } else if (!message.pathRequestRetried) {
                                // Never activated — retry the path request exactly once. (:2615-2618)
                                Transport.requestPath(message.destinationHash)
                                message.pathRequestRetried = true
                            }
                        }

                        // Link closed
                        directLinks.remove(destHashHex)
                        pendingLinkEstablishments.remove(destHashHex)

                        // Notify messages that need this link
                        handleLinkClosed(destHashHex)

                        // Symmetric with establishedCallback above —
                        // when the link watchdog tears down a stuck
                        // PENDING link, the message stays in OUTBOUND
                        // and `handleLinkClosed` skips it (it only
                        // resets SENDING). Without this kick the next
                        // retry waits silently for the periodic
                        // PROCESSING_INTERVAL (4s) tick. Restart the
                        // dispatch loop now so the no-link branch can
                        // bump deliveryAttempts and reach FAILED via
                        // MAX_DELIVERY_ATTEMPTS without 4s of dead air.
                        triggerProcessing()
                    },
                )

            // Store pending link
            directLinks[destHashHex] = link
        } catch (e: Exception) {
            pendingLinkEstablishments.remove(destHashHex)
            RnsLog.error("LXMRouter") { "Failed to establish link to $destHashHex: ${e.message}" }
            message.nextDeliveryAttempt = System.currentTimeMillis() + DELIVERY_RETRY_WAIT
        }
    }

    /**
     * Set up callbacks on an established link.
     */
    private fun setupLinkCallbacks(
        link: Link,
        destHashHex: String,
    ) {
        // Packet callback for receiving messages
        link.setPacketCallback { data, packet ->
            // Send proof back to sender for delivery confirmation
            packet.prove()
            processInboundDelivery(data, DeliveryMethod.DIRECT, null, link, sourcePacket = packet)
        }

        // Enable app-controlled resource acceptance for LXMF messages
        link.setResourceStrategy(Link.ACCEPT_APP)

        // Resource advertisement callback - accept all LXMF resources
        link.setResourceCallback { _: ResourceAdvertisement ->
            // Accept LXMF resources
            true
        }

        link.setResourceStartedCallback { _: Any ->
            RnsLog.debug("LXMRouter") { "Resource transfer started on link to $destHashHex" }
        }

        link.setResourceConcludedCallback { resource: Any ->
            handleResourceConcluded(resource, link)
        }
    }

    /**
     * Identify ourselves on a link (for backchannel replies).
     */
    private fun identifyOnLink(link: Link) {
        // Get our first delivery destination's identity
        val deliveryDest = deliveryDestinations.values.firstOrNull()
        if (deliveryDest == null) {
            RnsLog.debug("LXMRouter") { "identifyOnLink: NO delivery destinations registered!" }
            return
        }

        try {
            RnsLog.debug("LXMRouter") { "identifyOnLink: identifying as ${deliveryDest.identity.hexHash.take(12)}" }
            link.identify(deliveryDest.identity)
        } catch (e: Exception) {
            RnsLog.error("LXMRouter") { "identifyOnLink FAILED: ${e.message}" }
        }
    }

    /**
     * Handle a link being closed.
     */
    private fun handleLinkClosed(destHashHex: String) {
        processingScope.launch {
            pendingOutboundMutex.withLock {
                for (message in pendingOutbound) {
                    if (message.destinationHash.toHexString() == destHashHex &&
                        message.state == MessageState.SENDING
                    ) {
                        // Reset to outbound for retry
                        message.state = MessageState.OUTBOUND
                        message.nextDeliveryAttempt = System.currentTimeMillis() + DELIVERY_RETRY_WAIT
                    }
                }
            }
        }
    }

    /**
     * Handle a resource transfer being concluded.
     */
    private fun handleResourceConcluded(
        resource: Any,
        link: Link,
    ) {
        // Cast to Resource and extract data
        val res = resource as? Resource
        if (res == null) {
            RnsLog.debug("LXMRouter") { "Resource concluded but could not cast resource" }
            return
        }

        val data = res.data
        if (data == null || data.isEmpty()) {
            RnsLog.debug("LXMRouter") { "Resource concluded but no data received" }
            return
        }

        // Process as inbound LXMF delivery
        processInboundDelivery(data, DeliveryMethod.DIRECT, null, link)
    }

    /**
     * Process propagated message delivery.
     *
     * Matches Python LXMF LXMRouter.py lines 2669-2720:
     * - If link exists and ACTIVE -> send message
     * - If link exists and CLOSED -> clear link, set retry wait
     * - If link exists but PENDING -> just wait (no retry delay)
     * - If link is null -> establish new link (with retry wait check)
     */
    private suspend fun processPropagatedDelivery(message: LXMessage) {
        // Check for active propagation node (Python line 2669-2671)
        // If no node is configured, fail immediately — there's nothing to retry against
        val node = getActivePropagationNode()
        if (node == null) {
            message.state = MessageState.FAILED
            message.failedCallback?.invoke(message)
            return
        }

        // Check max delivery attempts (Python line 2673). Fire failedCallback so
        // upstream consumers (e.g. UI status updates, fallback policies) get notified.
        if (message.deliveryAttempts > MAX_DELIVERY_ATTEMPTS) {
            message.state = MessageState.FAILED
            message.failedCallback?.invoke(message)
            return
        }

        val link = outboundPropagationLink

        when {
            // Link exists and is ACTIVE -> send message (Python line 2678-2682)
            link != null && link.status == LinkConstants.ACTIVE -> {
                if (message.state != MessageState.SENDING) {
                    sendViaPropagation(message, link)
                }
                // If already SENDING, just wait for transfer to complete
            }

            // Link exists and is CLOSED -> clear and retry later (Python line 2688-2691)
            link != null && link.status == LinkConstants.CLOSED -> {
                outboundPropagationLink = null
                message.nextDeliveryAttempt = System.currentTimeMillis() + DELIVERY_RETRY_WAIT
            }

            // Link exists but is PENDING or other state -> just wait (Python line 2692-2695)
            link != null -> {
                // Don't set nextDeliveryAttempt - the established callback will trigger processOutbound
            }

            // No link exists -> establish one (Python line 2696-2715)
            else -> {
                // Only establish if we haven't tried recently (Python line 2700)
                val nextAttempt = message.nextDeliveryAttempt ?: 0L
                val now = System.currentTimeMillis()

                if (nextAttempt == 0L || now >= nextAttempt) {
                    message.deliveryAttempts++
                    message.nextDeliveryAttempt = now + DELIVERY_RETRY_WAIT

                    if (message.deliveryAttempts <= MAX_DELIVERY_ATTEMPTS) {
                        establishPropagationLink(node, forRetrieval = false)
                    } else {
                        message.state = MessageState.FAILED
                        message.failedCallback?.invoke(message)
                    }
                }
            }
        }
    }

    /**
     * Send a message via propagation node.
     */
    private fun sendViaPropagation(
        message: LXMessage,
        link: Link,
    ) {
        val packed = message.packed ?: return

        message.state = MessageState.SENDING
        message.method = DeliveryMethod.PROPAGATED

        try {
            // Build propagation format matching Python LXMessage.pack() lines 434-441:
            //   1. Encrypt: pn_encrypted = destination.encrypt(packed[DEST_LEN:])
            //   2. Build:   lxm_data = destHash + pn_encrypted
            //   3. Compute: transient_id = full_hash(lxm_data)
            //   4. Stamp:   propagation_stamp = generate_stamp(transient_id, cost)
            //   5. Wire:    transient_data = lxm_data + propagation_stamp
            //
            // Python's validate_pn_stamp() splits: lxm_data = data[:-STAMP_SIZE], stamp = data[-STAMP_SIZE:]
            // Then: transient_id = full_hash(lxm_data), workblock = stamp_workblock(transient_id)
            val destHash = packed.copyOfRange(0, LXMFConstants.DESTINATION_LENGTH)
            val plainData = packed.copyOfRange(LXMFConstants.DESTINATION_LENGTH, packed.size)

            val dest = message.destination
                ?: throw IllegalStateException("Cannot propagate without destination")
            val encryptedData = dest.encrypt(plainData)

            // lxm_data = destHash + encrypted (this is what transient_id is computed from)
            val lxmData = destHash + encryptedData

            // Compute transient_id and generate propagation stamp against it
            // (NOT against message.hash — the stamp must validate against transient_id)
            val transientId = network.reticulum.crypto.Hashes.fullHash(lxmData)
            val propStampResult = kotlinx.coroutines.runBlocking {
                LXStamper.generateStampWithWorkblock(
                    messageId = transientId,
                    stampCost = getActivePropagationNode()?.stampCost ?: 8,
                    expandRounds = LXStamper.WORKBLOCK_EXPAND_ROUNDS_PN,
                )
            }
            val propStamp = propStampResult.stamp
            RnsLog.debug("LXMRouter") { "Propagation stamp generated: value=${propStampResult.value}, cost=${getActivePropagationNode()?.stampCost}" }

            // Append propagation stamp (Python strips last STAMP_SIZE bytes before computing transient_id)
            val transientData = if (propStamp != null) lxmData + propStamp else lxmData

            // Pack for wire: msgpack([timebase, [transient_data, ...]])
            val buffer = java.io.ByteArrayOutputStream()
            val packer =
                org.msgpack.core.MessagePack
                    .newDefaultPacker(buffer)

            packer.packArrayHeader(2)
            packer.packDouble(System.currentTimeMillis() / 1000.0)

            // Message list with just our message
            packer.packArrayHeader(1)
            packer.packBinaryHeader(transientData.size)
            packer.writePayload(transientData)

            packer.close()

            // Send as Resource (propagation transfers are always Resource-based)
            val resource =
                Resource.create(
                    data = buffer.toByteArray(),
                    link = link,
                    callback = { _ ->
                        // Transfer complete - for propagated messages, SENT is final
                        message.state = MessageState.SENT
                        message.deliveryCallback?.invoke(message)
                    },
                    progressCallback = { progressResource ->
                        message.progress = progressResource.progress.toDouble()
                    },
                )

            // Track the resource
            val messageHashHex = message.hash?.toHexString() ?: ""
            resource.callbacks.failed = {
                pendingResources.remove(messageHashHex)

                retryAfterUnproven(message, "A resource to a propagation node failed")
            }
            pendingResources[messageHashHex] = Pair(message, resource)
        } catch (e: Exception) {
            RnsLog.error("LXMRouter") { "Failed to send via propagation: ${e.message}" }
            message.state = MessageState.OUTBOUND
            message.nextDeliveryAttempt = System.currentTimeMillis() + DELIVERY_RETRY_WAIT
        }
    }

    /**
     * Send a message via an established link.
     */
    private fun sendViaLink(
        message: LXMessage,
        link: Link,
    ) {
        val packed = message.packed ?: return

        message.state = MessageState.SENDING
        message.method = DeliveryMethod.DIRECT

        if (message.representation == MessageRepresentation.PACKET) {
            // For DIRECT delivery over link, Python's __as_packet sends full self.packed (WITH dest hash):
            //   elif self.method == LXMessage.DIRECT:
            //       return RNS.Packet(self.__delivery_destination, self.packed)
            // And unpack_from_bytes expects the destination hash at the start.
            val lxmfData = packed // Full packed message including destination hash
            // Send as packet over link with receipt tracking
            try {
                val receipt = link.sendWithReceipt(lxmfData)
                if (receipt != null) {
                    message.state = MessageState.SENT

                    // Set up delivery confirmation callback
                    receipt.setDeliveryCallback { _ ->
                        message.state = MessageState.DELIVERED
                        message.deliveryCallback?.invoke(message)
                    }

                    // Set up timeout callback
                    receipt.setTimeoutCallback { _ ->
                        retryAfterUnproven(message, "A packet over a link went unproven")
                    }
                } else {
                    // Send failed
                    message.state = MessageState.OUTBOUND
                    message.nextDeliveryAttempt = System.currentTimeMillis() + DELIVERY_RETRY_WAIT
                }
            } catch (e: Exception) {
                RnsLog.error("LXMRouter") { "Failed to send packet via link: ${e.message}" }
                message.state = MessageState.OUTBOUND
                message.nextDeliveryAttempt = System.currentTimeMillis() + DELIVERY_RETRY_WAIT
            }
        } else {
            // Send as resource for large messages
            // Python's __as_resource() sends self.packed (full message including dest hash)
            try {
                val messageHashHex = message.hash?.toHexString() ?: ""
                val resource =
                    Resource.create(
                        data = packed, // Full packed message, matches Python's __as_resource()
                        link = link,
                        callback = { completedResource ->
                            // Resource transfer complete
                            pendingResources.remove(messageHashHex)
                            message.state = MessageState.DELIVERED
                            message.progress = 1.0
                            message.deliveryCallback?.invoke(message)
                        },
                        progressCallback = { progressResource ->
                            // Update progress (Resource provides progress as 0.0-1.0 Float)
                            message.progress = progressResource.progress.toDouble()
                        },
                    )
                resource.callbacks.failed = {
                    pendingResources.remove(messageHashHex)

                    retryAfterUnproven(message, "A resource transfer failed")
                }

                // Track resource for completion
                pendingResources[messageHashHex] = Pair(message, resource)
                message.state = MessageState.SENDING
            } catch (e: Exception) {
                RnsLog.error("LXMRouter") { "Failed to send resource via link: ${e.message}" }
                message.state = MessageState.OUTBOUND
                message.nextDeliveryAttempt = System.currentTimeMillis() + DELIVERY_RETRY_WAIT
            }
        }
    }

    // ===== Inbound Message Handling =====

    /**
     * Handle an incoming delivery packet.
     */
    private fun handleDeliveryPacket(
        data: ByteArray,
        destination: Destination,
        packet: Packet?,
    ) {
        // CRITICAL: Send proof back to sender FIRST, before processing
        // This is what triggers delivery confirmation on the sender side
        packet?.prove()

        RnsLog.debug("LXMRouter") { "handleDeliveryPacket called with ${data.size} bytes for ${destination.hexHash}" }
        // For OPPORTUNISTIC delivery (single packet), the data doesn't include the
        // destination hash - we need to prepend it to match the LXMF message format.
        // Format: [destination_hash (16)] + [source_hash (16)] + [signature (64)] + [payload]
        val method = DeliveryMethod.OPPORTUNISTIC
        val lxmfData = destination.hash + data
        RnsLog.debug("LXMRouter") { "Prepended dest hash, lxmfData size: ${lxmfData.size} bytes (was ${data.size})" }

        // Process the delivery. `packet` carries the receive-time phy metadata
        // (rssi/snr/interface/hops) we want to surface on the LXMessage.
        processInboundDelivery(lxmfData, method, destination, sourcePacket = packet)
    }

    /**
     * Handle a new delivery link being established.
     */
    private fun handleDeliveryLinkEstablished(
        link: Link,
        destination: Destination,
    ) {
        // Set up link callbacks for packets
        link.setPacketCallback { data, packet ->
            // Send proof back to sender for delivery confirmation
            packet.prove()
            processInboundDelivery(data, DeliveryMethod.DIRECT, destination, link, sourcePacket = packet)
        }

        // Enable app-controlled resource acceptance for LXMF messages
        link.setResourceStrategy(Link.ACCEPT_APP)

        // Resource advertisement callback - accept all LXMF resources
        link.setResourceCallback { _: ResourceAdvertisement ->
            true
        }

        link.setResourceConcludedCallback { resource: Any ->
            handleResourceConcluded(resource, link)
        }

        // Set up callback for when the remote peer identifies themselves
        // This is critical for backchannel replies - the sender calls link.identify()
        // to reveal their identity so we can reply to them
        link.setRemoteIdentifiedCallback { _, remoteIdentity ->
            val remoteHashHex = remoteIdentity.hexHash
            RnsLog.debug("LXMRouter") { "Remote peer identified on link: $remoteHashHex" }

            // Calculate the LXMF delivery destination hash from the identity
            // This is the hash that will appear as sourceHash in LXMF messages
            val lxmfDestHash = Destination.hashFromNameAndIdentity("lxmf.delivery", remoteIdentity)
            val lxmfDestHashHex = lxmfDestHash.toHexString()

            // Store the backchannel link for replies (use LXMF dest hash, not identity hash)
            backchannelLinks[lxmfDestHashHex] = link

            // Store the identity so Identity.recall(sourceHash) works
            // This is needed because receivers use Identity.recall(message.sourceHash)
            // to get the sender's identity for replies
            Identity.remember(
                packetHash = link.hash, // Use link hash as packet hash
                destHash = lxmfDestHash, // Use LXMF destination hash as lookup key
                publicKey = remoteIdentity.getPublicKey(),
                appData = null,
            )
            RnsLog.debug("LXMRouter") { "Stored identity for LXMF dest: $lxmfDestHashHex" }
        }

        // Also check if identity is already known (for immediate identification)
        link.getRemoteIdentity()?.let { remoteIdentity ->
            val remoteHashHex = remoteIdentity.hexHash
            backchannelLinks[remoteHashHex] = link
        }
    }

    /**
     * Process an inbound LXMF delivery.
     *
     * @param data The raw message data
     * @param method The delivery method used
     * @param destination The destination that received the message
     * @param link Optional link if this came via direct delivery
     * @param sourcePacket Optional delivering packet. Provided for single-packet
     *   paths (OPPORTUNISTIC and single-packet DIRECT). Not available for
     *   Resource-delivered or propagation-fetched messages. Used to annotate
     *   the resulting LXMessage with receive-time phy metadata (rssi, snr,
     *   interface hash, hop count).
     */
    private fun processInboundDelivery(
        data: ByteArray,
        method: DeliveryMethod,
        destination: Destination? = null,
        link: Link? = null,
        sourcePacket: Packet? = null,
    ) {
        RnsLog.debug("LXMRouter") { "processInboundDelivery called with ${data.size} bytes, method=$method" }

        // Enforce incoming message size limit
        val limitKb = incomingMessageSizeLimitKb
        if (limitKb != null && data.size > limitKb * 1024) {
            RnsLog.debug("LXMRouter") { "Rejecting oversized message: ${data.size} bytes > ${limitKb}KB limit" }
            return
        }

        try {
            // For PROPAGATED messages from a propagation node, the data is encrypted:
            //   data = dest_hash(16) + encrypted(source_hash + signature + payload)
            // We need to decrypt using our delivery destination before unpacking.
            // Matches Python LXMRouter.lxmf_propagation() lines 2322-2328.
            val lxmfData = if (method == DeliveryMethod.PROPAGATED) {
                val destHash = data.copyOfRange(0, LXMFConstants.DESTINATION_LENGTH)
                val destHashHex = destHash.toHexString()
                val deliveryDest = deliveryDestinations[destHashHex]
                if (deliveryDest != null) {
                    val encryptedData = data.copyOfRange(LXMFConstants.DESTINATION_LENGTH, data.size)
                    RnsLog.debug("LXMRouter") { "Decrypting propagated message: encrypted=${encryptedData.size} bytes, identity.hasPrivateKey=${deliveryDest.destination.identity?.hasPrivateKey}" }
                    val decryptedData = deliveryDest.destination.decrypt(encryptedData)
                    if (decryptedData != null) {
                        RnsLog.debug("LXMRouter") { "Decrypted propagated message for $destHashHex" }
                        destHash + decryptedData
                    } else {
                        RnsLog.error("LXMRouter") { "Failed to decrypt propagated message for $destHashHex" }
                        return
                    }
                } else {
                    // Not addressed to us — shouldn't happen in normal flow
                    RnsLog.debug("LXMRouter") { "Propagated message not for us: $destHashHex" }
                    return
                }
            } else {
                data
            }

            // Unpack the message
            val message = LXMessage.unpackFromBytes(lxmfData, method)
            if (message == null) {
                RnsLog.error("LXMRouter") { "Failed to unpack LXMF message" }
                return
            }
            RnsLog.debug("LXMRouter") { "Unpacked message from ${message.sourceHash.toHexString()}" }

            message.incoming = true
            message.method = method

            // Check for duplicates. Dedup on message.hash (full hash of
            // destination_hash + source_hash + payload-without-stamp), which
            // is set unconditionally by LXMessage.unpackFromBytes:763. The
            // older code checked transientId, but transientId is only
            // populated on the SENDER side during outbound packing for
            // propagation (LXMessage.kt outbound → LXMessage.py:439); on the
            // incoming path it's always null, so the dedup check was a
            // perpetual no-op. This let DIRECT-delivery LXMessages large
            // enough to trigger Resource transfer (>319 bytes) deliver
            // twice — once via setPacketCallback firing on the final chunk,
            // once via setResourceConcludedCallback firing on assembly. See
            // LXMF-kt#8. Matches Python LXMRouter.py:1788/1792 which uses
            // message.hash as the locally_delivered_transient_ids key.
            val dedupKey = message.hash?.toHexString()
            if (dedupKey != null && locallyDeliveredTransientIds.containsKey(dedupKey)) {
                RnsLog.debug("LXMRouter") { "Duplicate message detected, ignoring" }
                return
            }

            // Validate signature if possible
            if (!message.signatureValidated) {
                when (message.unverifiedReason) {
                    UnverifiedReason.SOURCE_UNKNOWN -> {
                        // Source not known - could still accept depending on policy
                        RnsLog.debug("LXMRouter") { "Message from unknown source: ${message.sourceHash.toHexString()}" }
                    }
                    UnverifiedReason.SIGNATURE_INVALID -> {
                        RnsLog.warn("LXMRouter") { "Message signature invalid, rejecting" }
                        return
                    }
                    null -> {
                        // No error, signature validated
                    }
                }
            }

            // Check ignored list (access control)
            val sourceHashHexForCheck = message.sourceHash.toHexString()
            if (ignoredList.any { it.toHexString() == sourceHashHexForCheck }) {
                RnsLog.debug("LXMRouter") { "Ignored message from $sourceHashHexForCheck" }
                return
            }

            // Validate stamp if required (PAPER messages bypass stamp enforcement)
            val destHashHex = message.destinationHash.toHexString()
            val deliveryDest = deliveryDestinations[destHashHex]
            val requiredCost = deliveryDest?.stampCost
            val noStampEnforcement = method == DeliveryMethod.PAPER
            if (requiredCost != null && requiredCost > 0) {
                // Extract ticket from incoming message first
                extractAndRememberTicket(message)

                val tickets = getInboundTickets(sourceHashHexForCheck)
                if (!message.validateStamp(requiredCost, tickets)) {
                    if (noStampEnforcement) {
                        RnsLog.warn("LXMRouter") { "Message from $sourceHashHexForCheck has invalid stamp, but allowing (PAPER delivery)" }
                    } else {
                        RnsLog.error("LXMRouter") { "Message from $sourceHashHexForCheck failed stamp validation (required cost: $requiredCost)" }
                        return
                    }
                }
            }

            // Store backchannel link and identity for replies
            if (link != null) {
                val sourceHashHex = message.sourceHash.toHexString()
                backchannelLinks[sourceHashHex] = link

                // If the link has a remote identity, store it so Identity.recall() works
                // This is needed for echo/reply functionality
                link.getRemoteIdentity()?.let { remoteIdentity ->
                    // Calculate the LXMF delivery destination hash from the identity
                    val lxmfDestHash = Destination.hashFromNameAndIdentity("lxmf.delivery", remoteIdentity)

                    // Only store if the identity's LXMF dest hash matches the source hash
                    if (lxmfDestHash.contentEquals(message.sourceHash)) {
                        Identity.remember(
                            packetHash = link.hash,
                            destHash = lxmfDestHash,
                            publicKey = remoteIdentity.getPublicKey(),
                            appData = null,
                        )
                        RnsLog.debug("LXMRouter") { "Stored identity from link for LXMF dest: $sourceHashHex" }
                    }
                }
            }

            // Mark as delivered. Both keys (message.hash and, for
            // PROPAGATED/PAPER, the wire-source transient_id) are written
            // BEFORE the single saveTransientIdsAsync() call below — that
            // way the launched save coroutine reads a complete map at
            // execution time and a single file rewrite captures both
            // entries (vs two saves where the first could race ahead of
            // the second write). See dedupKey computation above for the
            // message.hash rationale, and #15 for the wire-source key
            // rationale (mirrors Python LXMRouter.py:1792 + 2320 — both
            // keys coexist in the heterogeneous-key dedup map).
            //
            //   PROPAGATED: the propagation node sends a list of
            //     transient_ids in its message-list response; the
            //     wantedIds filter at LXMRouter.kt:2231 queries by that
            //     transient_id. The propagation transient_id is
            //     full_hash(dest_hash + encrypted_payload) — i.e.
            //     full_hash(`data`) BEFORE we decrypt it locally.
            //
            //   PAPER: the paper-message ingestion path computes
            //     transient_id = full_hash(lxmfData) at LXMRouter.kt:3023
            //     before invoking processInboundDelivery; writing
            //     full_hash(`data`) makes that outer dedup actually fire
            //     on the second ingestion of the same paper message
            //     instead of always missing and falling through to the
            //     inner message.hash dedup.
            val nowSeconds = System.currentTimeMillis() / 1000
            var anyKeyWritten = false
            dedupKey?.let {
                locallyDeliveredTransientIds[it] = nowSeconds
                anyKeyWritten = true
            }
            if (method == DeliveryMethod.PROPAGATED || method == DeliveryMethod.PAPER) {
                val wireTransientIdHex = Hashes.fullHash(data).toHexString()
                locallyDeliveredTransientIds[wireTransientIdHex] = nowSeconds
                anyKeyWritten = true
            }
            if (anyKeyWritten) saveTransientIdsAsync()

            // Annotate the message with receive-time phy metadata from the
            // delivering packet / link. Propagation-fetched messages are
            // intentionally left null because the original in-path packet
            // context is lost; the sync link's values would refer to the
            // propagation node, not the original sender, and would be
            // misleading. (See LXMessage.receivedRssi KDoc.)
            //
            // `receivingInterfaceHash` is defensively copied on assignment
            // so that neither a caller mutating the delivered field nor the
            // RNS layer recycling a buffer can leak across the boundary.
            if (method != DeliveryMethod.PROPAGATED) {
                if (sourcePacket != null) {
                    // Single-packet path (OPPORTUNISTIC or small DIRECT): the
                    // delivering packet carries authoritative phy metadata.
                    message.receivedRssi = sourcePacket.rssi
                    message.receivedSnr = sourcePacket.snr
                    message.receivingInterfaceHash = sourcePacket.receivingInterfaceHash?.copyOf()
                    message.receivedHopCount = sourcePacket.hops
                } else if (link != null) {
                    // Resource-delivered path: no single source packet is
                    // available, but the Link aggregates phy stats from its
                    // constituent packets (updatePhyStats fires on each
                    // inbound packet on the link). The values reflect the
                    // most recent packet on the link, which for a Resource
                    // concluding at this moment is the final constituent
                    // packet — exactly what we want. Note: getRssi/getSnr
                    // return null unless trackPhyStats(true) has been
                    // enabled on the link by the caller.
                    message.receivedRssi = link.getRssi()
                    message.receivedSnr = link.getSnr()
                    message.receivingInterfaceHash = link.attachedInterfaceHash?.copyOf()
                    // Every constituent packet of a Resource traverses the
                    // same hop path as the link itself, so expectedHops is
                    // the correct hop count for the delivered message.
                    message.receivedHopCount = link.expectedHops
                }
                // else: no packet and no link — can happen if a caller
                // threads data in via a non-standard path; leave fields null.
            }

            // Invoke delivery callback
            deliveryCallback?.invoke(message)
        } catch (e: Exception) {
            RnsLog.error("LXMRouter") { "Error processing inbound delivery: ${e.message}" }
        }
    }

    // ===== Announce Handling =====

    /**
     * Pack app data for announcements.
     */
    private fun packAnnounceAppData(
        displayName: String?,
        stampCost: Int,
    ): ByteArray {
        val buffer = java.io.ByteArrayOutputStream()
        val packer =
            org.msgpack.core.MessagePack
                .newDefaultPacker(buffer)

        packer.packArrayHeader(2)

        // Display name
        if (displayName != null) {
            val nameBytes = displayName.toByteArray(Charsets.UTF_8)
            packer.packBinaryHeader(nameBytes.size)
            packer.writePayload(nameBytes)
        } else {
            packer.packNil()
        }

        // Stamp cost
        if (stampCost > 0) {
            packer.packInt(stampCost)
        } else {
            packer.packNil()
        }

        packer.close()
        return buffer.toByteArray()
    }

    /**
     * Handle a delivery announcement from a remote destination.
     */
    fun handleDeliveryAnnounce(
        destHash: ByteArray,
        appData: ByteArray?,
    ) {
        if (appData == null || appData.isEmpty()) {
            return
        }

        try {
            val unpacker =
                org.msgpack.core.MessagePack
                    .newDefaultUnpacker(appData)
            val arraySize = unpacker.unpackArrayHeader()

            if (arraySize >= 2) {
                // Skip display name
                unpacker.skipValue()

                // Extract stamp cost
                if (!unpacker.tryUnpackNil()) {
                    val stampCost = unpacker.unpackInt()
                    updateStampCost(destHash.toHexString(), stampCost)
                }
            }
        } catch (e: Exception) {
            RnsLog.error("LXMRouter") { "Error parsing delivery announce: ${e.message}" }
        }

        // Trigger retry for pending DIRECT/OPPORTUNISTIC messages to this
        // destination. Mirrors Python LXMFDeliveryAnnounceHandler.received_announce
        // (Handlers.py:LXMFDeliveryAnnounceHandler):
        //
        //     for lxmessage in self.lxmrouter.pending_outbound:
        //         if destination_hash     == lxmessage.destination_hash:
        //             if lxmessage.method == LXMessage.DIRECT or lxmessage.method == LXMessage.OPPORTUNISTIC:
        //                 lxmessage.next_delivery_attempt = time.time()
        //                 ... process_outbound()
        //
        // Filtering by method matters: PROPAGATED messages have their own
        // delivery driver (link to a propagation node, not to the destination
        // hash that just announced) and re-triggering them on a delivery
        // announce can cause spurious dispatch — see PropagatedDeliveryTest
        // regressions when this filter was missing.
        val destHashHex = destHash.toHexString()
        processingScope.launch {
            pendingOutboundMutex.withLock {
                for (message in pendingOutbound) {
                    if (message.destinationHash.toHexString() == destHashHex &&
                        (message.method == DeliveryMethod.DIRECT ||
                            message.method == DeliveryMethod.OPPORTUNISTIC)
                    ) {
                        message.nextDeliveryAttempt = System.currentTimeMillis()
                    }
                }
            }
            processOutbound()
        }
    }

    // ===== Router Lifecycle =====

    /**
     * Start the router processing loop.
     */
    fun start() {
        if (running) return

        // Load persisted propagation nodes from disk
        loadPropagationNodes()

        running = true
        // processingScope is now process-lifetime (constructed at router init);
        // start() no longer assigns it. The periodic loop below is still gated
        // by the `running` flag and tracked via processingJob for cancellation.
        processingJob =
            processingScope.launch {
                while (running) {
                    try {
                        processOutbound()
                        processDeferredStamps()

                        // Periodic cleanup every 60 iterations (~240 seconds at 4s interval)
                        processingCount++
                        if (processingCount % 60 == 0L) {
                            cleanTransientIdCaches()
                            cleanOutboundStampCosts()
                            cleanAvailableTickets()
                        }
                    } catch (e: Exception) {
                        RnsLog.error("LXMRouter") { "Error in processing loop: ${e.message}" }
                    }
                    delay(PROCESSING_INTERVAL)
                }
            }
    }

    /**
     * Stop the router processing loop.
     */
    fun stop() {
        running = false
        // Cancel the periodic processing loop — but NOT the scope itself.
        // Keeping processingScope alive across stop() means a subsequent
        // handleOutbound() call will still dispatch via triggerProcessing,
        // instead of silently enqueueing a message with no coroutine to
        // ever run it. See the processingScope KDoc for the 2026-04-21
        // silent-drop regression that motivated this change.
        processingJob?.cancel()
    }

    /**
     * Release the router's [processingScope] and the periodic processing
     * loop. Intended for non-singleton use — tests that create many routers,
     * or any caller that rebuilds the router after a reticulum-subprocess
     * restart. After [close] the router must not be reused; any call to
     * [handleOutbound] or [triggerProcessing] will no-op silently because
     * the scope's [SupervisorJob] has been cancelled.
     *
     * For singleton Android use the default [stop] is sufficient; calling
     * [close] is optional.
     */
    override fun close() {
        stop()
        processingScope.cancel()
    }

    // ===== Utility Methods =====

    /**
     * Get the number of pending outbound messages.
     */
    suspend fun pendingOutboundCount(): Int =
        pendingOutboundMutex.withLock {
            pendingOutbound.size
        }

    /**
     * Get the number of failed outbound messages.
     */
    suspend fun failedOutboundCount(): Int =
        failedOutboundMutex.withLock {
            failedOutbound.size
        }

    /**
     * Get a registered delivery destination by hash.
     */
    fun getDeliveryDestination(hexHash: String): DeliveryDestination? = deliveryDestinations[hexHash]

    /**
     * Get all registered delivery destinations.
     */
    fun getDeliveryDestinations(): List<DeliveryDestination> = deliveryDestinations.values.toList()

    /**
     * Set the inbound stamp cost for a registered delivery destination.
     *
     * Matches Python LXMRouter.set_inbound_stamp_cost() (lines 993-1001):
     * Validates cost range (null or 1-254) and updates the delivery destination.
     *
     * @param destinationHexHash The hex hash of the delivery destination
     * @param cost The stamp cost (null to disable, 1-254 for PoW requirement)
     * @throws IllegalArgumentException if cost is out of range or destination not found
     */
    fun setInboundStampCost(
        destinationHexHash: String,
        cost: Int?,
    ) {
        if (cost != null && (cost < 1 || cost > 254)) {
            throw IllegalArgumentException("Stamp cost must be null or between 1 and 254, got $cost")
        }

        val deliveryDest =
            deliveryDestinations[destinationHexHash]
                ?: throw IllegalArgumentException("No delivery destination registered with hash $destinationHexHash")

        deliveryDest.stampCost = cost

        // Update announce app data with new stamp cost
        val appData = packAnnounceAppData(deliveryDest.displayName, cost ?: 0)
        deliveryDest.destination.setDefaultAppData(appData)
    }

    /**
     * Announce a delivery destination.
     */
    fun announce(
        destination: Destination,
        appData: ByteArray? = null,
    ) {
        destination.announce(appData)
    }

    // ===== Propagation Node Methods =====

    /**
     * Handle a propagation node announcement.
     *
     * Parses the announcement app data and stores the node information
     * for future use when sending PROPAGATED messages.
     *
     * @param destHash The destination hash of the propagation node
     * @param identity The identity of the propagation node
     * @param appData The announcement app data (msgpack-encoded)
     */
    fun handlePropagationAnnounce(
        destHash: ByteArray,
        identity: Identity,
        appData: ByteArray?,
    ) {
        if (appData == null || appData.isEmpty()) {
            return
        }

        try {
            val unpacker =
                org.msgpack.core.MessagePack
                    .newDefaultUnpacker(appData)
            val arraySize = unpacker.unpackArrayHeader()

            if (arraySize < 7) {
                RnsLog.warn("LXMRouter") { "Invalid propagation announce: expected 7 fields, got $arraySize" }
                return
            }

            // [0] Legacy support flag (ignored)
            unpacker.skipValue()

            // [1] Timebase (int - unix timestamp)
            val timebase = if (!unpacker.tryUnpackNil()) unpackAsLong(unpacker) else 0L

            // [2] Node state (bool - active propagation node)
            val isActive = if (!unpacker.tryUnpackNil()) unpacker.unpackBoolean() else true

            // [3] Per-transfer limit (int KB) — some nodes send as float
            val perTransferLimit =
                if (!unpacker.tryUnpackNil()) {
                    unpackAsInt(unpacker)
                } else {
                    LXMFConstants.PROPAGATION_LIMIT_KB
                }

            // [4] Per-sync limit (int KB) — some nodes send as float
            val perSyncLimit =
                if (!unpacker.tryUnpackNil()) {
                    unpackAsInt(unpacker)
                } else {
                    LXMFConstants.SYNC_LIMIT_KB
                }

            // [5] Stamp cost list [cost, flexibility, peering_cost]
            var stampCost = LXMFConstants.PROPAGATION_COST
            var stampCostFlex = LXMFConstants.PROPAGATION_COST_FLEX
            var peeringCost = LXMFConstants.PEERING_COST

            if (!unpacker.tryUnpackNil()) {
                val costArraySize = unpacker.unpackArrayHeader()
                if (costArraySize >= 1) stampCost = unpackAsInt(unpacker)
                if (costArraySize >= 2) stampCostFlex = unpackAsInt(unpacker)
                if (costArraySize >= 3) peeringCost = unpackAsInt(unpacker)
            }

            // [6] Metadata (map with node name, etc.)
            var displayName: String? = null
            if (!unpacker.tryUnpackNil()) {
                val metaSize = unpacker.unpackMapHeader()
                for (i in 0 until metaSize) {
                    val key = unpacker.unpackInt()
                    when (key) {
                        LXMFConstants.PN_META_NAME -> {
                            if (!unpacker.tryUnpackNil()) {
                                val nameLen = unpacker.unpackBinaryHeader()
                                val nameBytes = ByteArray(nameLen)
                                unpacker.readPayload(nameBytes)
                                displayName = String(nameBytes, Charsets.UTF_8)
                            }
                        }
                        else -> unpacker.skipValue()
                    }
                }
            }

            unpacker.close()

            // Create and store the propagation node
            val node =
                PropagationNode(
                    destHash = destHash,
                    identity = identity,
                    displayName = displayName,
                    timebase = timebase,
                    isActive = isActive,
                    perTransferLimit = perTransferLimit,
                    perSyncLimit = perSyncLimit,
                    stampCost = stampCost,
                    stampCostFlexibility = stampCostFlex,
                    peeringCost = peeringCost,
                )

            propagationNodes[node.hexHash] = node
            savePropagationNodes()
            RnsLog.debug("LXMRouter") { "Discovered propagation node: ${node.hexHash} (${displayName ?: "unnamed"})" }
        } catch (e: Exception) {
            RnsLog.error("LXMRouter") { "Error parsing propagation announce: ${e.message}" }
        }
    }

    /**
     * Set the active propagation node for outbound PROPAGATED messages.
     *
     * @param destHashHex The hex-encoded destination hash of the propagation node
     * @return True if the node was set successfully
     */
    fun setActivePropagationNode(destHashHex: String): Boolean {
        var node = propagationNodes[destHashHex]

        // If node isn't in the in-memory map (announce hasn't arrived yet),
        // try to recall the identity from Transport and create a minimal entry.
        // The full details will be populated when the announce arrives.
        if (node == null) {
            val destHash = destHashHex.chunked(2).map { it.toInt(16).toByte() }.toByteArray()
            val recalledIdentity = Identity.recall(destHash)
            if (recalledIdentity != null) {
                node =
                    PropagationNode(
                        destHash = destHash,
                        identity = recalledIdentity,
                        isActive = true,
                    )
                propagationNodes[destHashHex] = node
                RnsLog.debug("LXMRouter") { "Created minimal propagation node entry from recalled identity: $destHashHex" }
            } else {
                RnsLog.error("LXMRouter") { "Cannot set propagation node: no announce and no recalled identity for $destHashHex" }
                // Still save the hash — the announce may arrive later
                activePropagationNodeHash = destHashHex
                return true
            }
        }

        activePropagationNodeHash = destHashHex
        // Close existing link if any
        outboundPropagationLink?.teardown()
        outboundPropagationLink = null

        RnsLog.debug("LXMRouter") { "Active propagation node set to: $destHashHex" }
        return true
    }

    /**
     * Clear the active propagation node.
     */
    fun clearActivePropagationNode() {
        activePropagationNodeHash = null
        outboundPropagationLink?.teardown()
        outboundPropagationLink = null
    }

    /**
     * Get the active propagation node.
     */
    fun getActivePropagationNode(): PropagationNode? {
        val hash = activePropagationNodeHash ?: return null
        propagationNodes[hash]?.let { return it }

        // Node not in map — try late recall (identities may have loaded after setActive was called)
        val destHash = hash.chunked(2).map { it.toInt(16).toByte() }.toByteArray()
        val identity = Identity.recall(destHash) ?: return null
        val node = PropagationNode(destHash = destHash, identity = identity, isActive = true)
        propagationNodes[hash] = node
        RnsLog.debug("LXMRouter") { "Late-recalled propagation node identity for $hash" }
        return node
    }

    /**
     * Get all known propagation nodes.
     */
    fun getPropagationNodes(): List<PropagationNode> = propagationNodes.values.filter { it.isActive }.toList()

    /**
     * Add a propagation node directly to the known nodes map.
     *
     * This method bypasses the announce flow and is useful for testing
     * or when the propagation node details are known through other means.
     *
     * @param node The propagation node to add
     */
    fun addPropagationNode(node: PropagationNode) {
        propagationNodes[node.hexHash] = node
        RnsLog.debug("LXMRouter") { "Added propagation node: ${node.hexHash} (${node.displayName ?: "unnamed"})" }
    }

    /**
     * Get the stamp cost required for the active propagation node.
     *
     * @return The minimum stamp cost, or null if no active node
     */
    fun getOutboundPropagationCost(): Int? {
        val node = getActivePropagationNode() ?: return null
        // Minimum accepted cost is (cost - flexibility), but at least PROPAGATION_COST_MIN
        return maxOf(
            LXMFConstants.PROPAGATION_COST_MIN,
            node.stampCost - node.stampCostFlexibility,
        )
    }

    /**
     * Request messages from the active propagation node.
     *
     * This initiates the two-stage retrieval process:
     * 1. List available messages
     * 2. Download selected messages
     *
     * Progress can be tracked via propagationTransferProgress and
     * results via propagationTransferLastResult.
     */
    fun requestMessagesFromPropagationNode() {
        val node = getActivePropagationNode()
        if (node == null) {
            // Identity not available yet — request path so it arrives for next cycle
            val hash = activePropagationNodeHash
            if (hash != null) {
                val destHash = hash.chunked(2).map { it.toInt(16).toByte() }.toByteArray()
                Transport.requestPath(destHash)
                RnsLog.debug("LXMRouter") { "Propagation node identity not available, requested path for $hash" }
            }
            propagationTransferState = PropagationTransferState.FAILED
            return
        }

        propagationTransferState = PropagationTransferState.LINK_ESTABLISHING
        propagationTransferProgress = 0.0
        propagationTransferLastResult = 0

        // Check if we have an active link
        val link = outboundPropagationLink
        RnsLog.debug("LXMRouter") { "requestMessages: link=${link != null}, status=${link?.status}, node=${node.hexHash.take(12)}" }
        if (link != null && link.status == LinkConstants.ACTIVE) {
            propagationTransferState = PropagationTransferState.LINK_ESTABLISHED
            // Mirror python LXMRouter.py:494 — identify on the link before
            // requesting messages, even when reusing an existing one. The
            // delivery path establishes a link without identifying, so a
            // reused link won't be associated with our peer identity until
            // we call identify here. Skipping this caused lxmd to return
            // ERROR_NO_IDENTITY (240) on /get requests.
            identifyOnLink(link)
            requestMessageList(link)
        } else {
            // Need to establish link first
            establishPropagationLink(node)
        }
        RnsLog.debug("LXMRouter") { "requestMessages: returning, state=$propagationTransferState" }
    }

    /**
     * Establish a link to a propagation node.
     *
     * @param node The propagation node to connect to
     * @param forRetrieval True for message retrieval (calls requestMessageList),
     *                     False for message delivery (calls processOutbound).
     *                     Matches Python's architecture where delivery path (line 2709) uses
     *                     established_callback=self.process_outbound, while retrieval path
     *                     (line 512) uses msg_request_established_callback that calls
     *                     request_messages_from_propagation_node.
     */
    private fun establishPropagationLink(
        node: PropagationNode,
        forRetrieval: Boolean = true,
    ) {
        try {
            // Create destination for the propagation node
            val destination =
                Destination.create(
                    identity = node.identity,
                    direction = DestinationDirection.OUT,
                    type = DestinationType.SINGLE,
                    appName = APP_NAME,
                    PROPAGATION_ASPECT,
                )
            RnsLog.debug("LXMRouter") { "establishPropagationLink: dest=${destination.hexHash}, hasPath=${Transport.hasPath(node.destHash)}" }

            val link =
                Link.create(
                    destination = destination,
                    establishedCallback = { establishedLink ->
                        RnsLog.debug("LXMRouter") { "establishPropagationLink: LINK ESTABLISHED!" }
                        outboundPropagationLink = establishedLink
                        propagationTransferState = PropagationTransferState.LINK_ESTABLISHED

                        if (forRetrieval) {
                            // Retrieval path identifies on the link so the PN
                            // can locate the requesting peer's queued messages.
                            // Mirrors python `request_messages_from_propagation_node`
                            // at LXMRouter.py:493 (`self.outbound_propagation_link
                            // .identify(identity)`). The delivery path
                            // intentionally does NOT identify — see the
                            // matching comment below for rationale.
                            identifyOnLink(establishedLink)

                            // Retrieval path: request message list (Python line 510)
                            // The identify packet may not have been processed yet by the
                            // remote node. If we get ERROR_NO_IDENTITY, handleMessageListResponse
                            // will retry after a brief delay to let identify propagate.
                            RnsLog.debug("LXMRouter") { "Calling requestMessageList on established link" }
                            requestMessageList(establishedLink)
                        } else {
                            // Delivery path: do NOT identify on the link.
                            // Mirrors python `process_outbound` for PROPAGATED
                            // at LXMRouter.py:2700-2704 — python establishes
                            // `RNS.Link(propagation_node_destination, ...)`
                            // and goes straight to sending the resource without
                            // calling `link.identify(...)`.
                            //
                            // Identifying on the delivery link causes lxmd's
                            // `propagation_resource_concluded` (LXMRouter.py:
                            // 2188-2214) to run the peer-discovery branch
                            // (`remote_destination = RNS.Destination(remote_identity,
                            // OUT, SINGLE, "lxmf", "propagation")` then
                            // `recall_app_data(remote_hash)`), which probes
                            // global state during the resource conclusion path
                            // and can stall the proof emission. Python avoids
                            // this by leaving the link unidentified for
                            // delivery — the message itself carries enough
                            // routing info via its embedded recipient
                            // destination hash.
                            // Delivery path: re-trigger outbound processing for pending messages (Python line 2709)
                            processingScope.launch {
                                // Reset nextDeliveryAttempt for pending PROPAGATED messages so they get processed immediately.
                                // This matches Python's behavior where process_outbound sends immediately when link is active,
                                // without checking next_delivery_attempt (which was set when we initiated link establishment).
                                pendingOutboundMutex.withLock {
                                    val now = System.currentTimeMillis()
                                    for (message in pendingOutbound) {
                                        if (message.desiredMethod == DeliveryMethod.PROPAGATED && message.state == MessageState.OUTBOUND) {
                                            message.nextDeliveryAttempt = now - 1 // Make eligible for immediate processing
                                        }
                                    }
                                }
                                processOutbound()
                            }
                        }
                    },
                    closedCallback = { _ ->
                        if (propagationTransferState != PropagationTransferState.COMPLETE) {
                            propagationTransferState = PropagationTransferState.NO_LINK
                        }
                        outboundPropagationLink = null
                    },
                )

            outboundPropagationLink = link
        } catch (e: Exception) {
            RnsLog.error("LXMRouter") { "Failed to establish propagation link: ${e.message}" }
            e.printStackTrace()
            propagationTransferState = PropagationTransferState.FAILED
        }
    }

    /**
     * Request the list of available messages from a propagation node.
     */
    private fun requestMessageList(link: Link) {
        propagationTransferState = PropagationTransferState.LISTING_MESSAGES
        RnsLog.debug("LXMRouter") { "requestMessageList: link.status=${link.status}, path=${LXMFConstants.MESSAGE_GET_PATH}" }

        try {
            // Send LIST request: [None, None]
            // Python sends [None, None] as data, which gets packed by Link.request
            val requestData = listOf(null, null)

            val receipt =
                link.request(
                    path = LXMFConstants.MESSAGE_GET_PATH,
                    data = requestData,
                    responseCallback = { receipt ->
                        val responseData = receipt.response
                        RnsLog.debug("LXMRouter") {
                            "messageList response: size=${responseData?.size}, " +
                                "hex=${responseData?.take(20)?.joinToString("") { "%02x".format(it) }}"
                        }
                        if (responseData != null) {
                            handleMessageListResponse(link, responseData)
                        } else {
                            propagationTransferState = PropagationTransferState.FAILED
                            RnsLog.debug("LXMRouter") { "Message list request returned null response" }
                        }
                    },
                    failedCallback = { _ ->
                        propagationTransferState = PropagationTransferState.FAILED
                        RnsLog.error("LXMRouter") { "Message list request failed" }
                    },
                )
            RnsLog.debug("LXMRouter") { "requestMessageList: receipt=$receipt" }
        } catch (e: Exception) {
            RnsLog.debug("LXMRouter") { "requestMessageList EXCEPTION: ${e.message}" }
            e.printStackTrace()
            propagationTransferState = PropagationTransferState.FAILED
        }
    }

    /** Track retry count for message list requests (reset per sync cycle) */
    private var messageListRetryCount = 0
    private val MAX_MESSAGE_LIST_RETRIES = 3
    private val MESSAGE_LIST_RETRY_DELAY_MS = 200L

    /**
     * Handle the message list response from a propagation node.
     */
    private fun handleMessageListResponse(
        link: Link,
        response: ByteArray,
    ) {
        try {
            val unpacker =
                org.msgpack.core.MessagePack
                    .newDefaultUnpacker(response)

            // Check for error response (Python returns int error codes)
            if (unpacker.nextFormat.valueType == org.msgpack.value.ValueType.INTEGER) {
                val errorCode = unpacker.unpackInt()
                // ERROR_NO_IDENTITY = identity not yet processed on remote.
                // This happens when requestMessageList fires before the LINKIDENTIFY
                // packet has been processed. Retry with backoff.
                if (messageListRetryCount < MAX_MESSAGE_LIST_RETRIES) {
                    messageListRetryCount++
                    RnsLog.error("LXMRouter") { "Propagation node returned error $errorCode, retrying ($messageListRetryCount/$MAX_MESSAGE_LIST_RETRIES)..." }
                    processingScope.launch {
                        kotlinx.coroutines.delay(MESSAGE_LIST_RETRY_DELAY_MS * messageListRetryCount)
                        requestMessageList(link)
                    }
                } else {
                    RnsLog.error("LXMRouter") { "Propagation node returned error $errorCode after $MAX_MESSAGE_LIST_RETRIES retries" }
                    propagationTransferState = PropagationTransferState.FAILED
                    messageListRetryCount = 0
                }
                return
            }
            messageListRetryCount = 0

            // Response is list of transient_ids (just the IDs, no sizes)
            // Python's message_get_request returns [transient_id, transient_id, ...]
            val messageCount = unpacker.unpackArrayHeader()
            val wantedIds = mutableListOf<ByteArray>()

            for (i in 0 until messageCount) {
                val idLen = unpacker.unpackBinaryHeader()
                val transientId = ByteArray(idLen)
                unpacker.readPayload(transientId)

                // Check if we already have this message
                val idHex = transientId.toHexString()
                if (!locallyDeliveredTransientIds.containsKey(idHex)) {
                    wantedIds.add(transientId)
                }
            }

            unpacker.close()

            if (wantedIds.isEmpty()) {
                propagationTransferState = PropagationTransferState.COMPLETE
                propagationTransferProgress = 1.0
                propagationTransferLastResult = 0
                RnsLog.debug("LXMRouter") { "No new messages from propagation node" }
                return
            }

            // Request the messages we want
            requestMessages(link, wantedIds)
        } catch (e: Exception) {
            RnsLog.error("LXMRouter") { "Error parsing message list: ${e.message}" }
            propagationTransferState = PropagationTransferState.FAILED
        }
    }

    /**
     * Request specific messages from a propagation node.
     */
    private fun requestMessages(
        link: Link,
        wantedIds: List<ByteArray>,
    ) {
        propagationTransferState = PropagationTransferState.REQUESTING_MESSAGES

        try {
            // Send GET request: [wants, haves, limit]
            // Python expects: data[0]=wants (list of transient IDs), data[1]=haves (list), data[2]=limit (int KB)
            val requestData =
                listOf(
                    wantedIds, // wants: list of transient IDs (ByteArray)
                    emptyList<ByteArray>(), // haves: empty list
                    LXMFConstants.DELIVERY_LIMIT_KB, // limit: KB limit
                )

            link.request(
                path = LXMFConstants.MESSAGE_GET_PATH,
                data = requestData,
                responseCallback = { receipt ->
                    val responseData = receipt.response
                    if (responseData != null) {
                        handleMessageGetResponse(link, responseData, wantedIds.size)
                    } else {
                        propagationTransferState = PropagationTransferState.FAILED
                        RnsLog.debug("LXMRouter") { "Message get request returned null response" }
                    }
                },
                failedCallback = { _ ->
                    propagationTransferState = PropagationTransferState.FAILED
                    RnsLog.error("LXMRouter") { "Message get request failed" }
                },
            )
        } catch (e: Exception) {
            RnsLog.error("LXMRouter") { "Failed to request messages: ${e.message}" }
            propagationTransferState = PropagationTransferState.FAILED
        }
    }

    /**
     * Handle the message get response from a propagation node.
     */
    private fun handleMessageGetResponse(
        link: Link,
        response: ByteArray,
        expectedCount: Int,
    ) {
        propagationTransferState = PropagationTransferState.RECEIVING_MESSAGES

        try {
            val unpacker =
                org.msgpack.core.MessagePack
                    .newDefaultUnpacker(response)

            // Check for error response
            if (unpacker.nextFormat.valueType == org.msgpack.value.ValueType.INTEGER) {
                val errorCode = unpacker.unpackInt()
                RnsLog.error("LXMRouter") { "Propagation node returned error: $errorCode" }
                propagationTransferState = PropagationTransferState.FAILED
                return
            }

            // Response is list of message data (raw bytes, no wrapper arrays)
            // Python's message_get_request returns [lxmf_data, lxmf_data, ...]
            val messageCount = unpacker.unpackArrayHeader()
            var receivedCount = 0
            val receivedHashes = mutableListOf<ByteArray>()

            for (i in 0 until messageCount) {
                val dataLen = unpacker.unpackBinaryHeader()
                val messageData = ByteArray(dataLen)
                unpacker.readPayload(messageData)

                // Track transient ID for deletion acknowledgment (Python line 1561)
                receivedHashes.add(
                    network.reticulum.crypto.Hashes
                        .fullHash(messageData),
                )

                // Process the message
                processInboundDelivery(messageData, DeliveryMethod.PROPAGATED)

                receivedCount++
                propagationTransferProgress = receivedCount.toDouble() / expectedCount
            }

            unpacker.close()

            // Send third request to acknowledge/delete received messages on the node
            // Matches Python LXMRouter.py:1566-1572
            if (receivedHashes.isNotEmpty()) {
                try {
                    link.request(
                        path = LXMFConstants.MESSAGE_GET_PATH,
                        data = listOf(null, receivedHashes),
                        failedCallback = { _ ->
                            RnsLog.error("LXMRouter") { "Failed to send deletion acknowledgment to propagation node" }
                        },
                    )
                } catch (e: Exception) {
                    RnsLog.error("LXMRouter") { "Error sending deletion acknowledgment: ${e.message}" }
                }
            }

            propagationTransferState = PropagationTransferState.COMPLETE
            propagationTransferProgress = 1.0
            propagationTransferLastResult = receivedCount

            RnsLog.debug("LXMRouter") { "Received $receivedCount messages from propagation node" }
        } catch (e: Exception) {
            RnsLog.error("LXMRouter") { "Error processing received messages: ${e.message}" }
            propagationTransferState = PropagationTransferState.FAILED
        }
    }

    // ===== Stamp Cost Management =====

    /**
     * Update outbound stamp cost for a destination.
     * Matches Python LXMRouter.update_stamp_cost() (lines 977-982).
     */
    private fun updateStampCost(
        destHashHex: String,
        stampCost: Int,
    ) {
        val nowSeconds = System.currentTimeMillis() / 1000
        outboundStampCosts[destHashHex] = Pair(nowSeconds, stampCost)
        saveOutboundStampCostsAsync()
    }

    // ===== Deferred Stamp Processing (Phase 2) =====

    /**
     * Process deferred stamp generation.
     *
     * Picks one pending deferred message, generates its stamp in a coroutine,
     * re-packs, and moves to pendingOutbound.
     */
    private suspend fun processDeferredStamps() {
        if (pendingDeferredStamps.isEmpty()) return

        stampGenMutex.withLock {
            // Pick first entry
            val entry = pendingDeferredStamps.entries.firstOrNull() ?: return
            val messageIdHex = entry.key
            val message = entry.value

            try {
                // Generate stamp
                message.getStamp()
                message.repackWithStamp()

                // Move to outbound queue
                pendingDeferredStamps.remove(messageIdHex)
                pendingOutboundMutex.withLock {
                    pendingOutbound.add(message)
                }
                triggerProcessing()
            } catch (e: Exception) {
                RnsLog.error("LXMRouter") { "Error generating deferred stamp for $messageIdHex: ${e.message}" }
                // Leave in deferred queue for retry
            }
        }
    }

    // ===== Ticket System (Phase 3) =====

    /**
     * Generate a ticket for a destination.
     *
     * Matches Python LXMRouter.generate_ticket() (lines 1022-1049):
     * - Check TICKET_INTERVAL between deliveries
     * - Reuse existing ticket if > TICKET_RENEW validity left
     * - Otherwise generate 32 random bytes
     *
     * @param destinationHash Raw destination hash bytes
     * @return [expires, ticketBytes] list for FIELD_TICKET, or null
     */
    private fun generateTicket(destinationHash: ByteArray): List<Any>? {
        val destHashHex = destinationHash.toHexString()
        val nowSeconds = System.currentTimeMillis() / 1000

        // Check if we delivered a ticket recently
        val lastDelivery = lastTicketDeliveries[destHashHex]
        if (lastDelivery != null) {
            val elapsed = nowSeconds - lastDelivery
            if (elapsed < LXMFConstants.TICKET_INTERVAL) {
                return null
            }
        }

        // Check for existing reusable ticket
        val existingTickets = inboundTickets[destHashHex]
        if (existingTickets != null) {
            for ((ticketHex, expires) in existingTickets) {
                val validityLeft = expires - nowSeconds
                if (validityLeft > LXMFConstants.TICKET_RENEW) {
                    // Reuse existing ticket
                    return listOf(expires, hexToBytes(ticketHex))
                }
            }
        }

        // Generate new ticket
        val expires = nowSeconds + LXMFConstants.TICKET_EXPIRY
        val ticket = ByteArray(RnsConstants.TRUNCATED_HASH_BYTES)
        SecureRandom().nextBytes(ticket)

        // Store in inbound tickets
        val tickets = inboundTickets.getOrPut(destHashHex) { ConcurrentHashMap() }
        tickets[ticket.toHexString()] = expires
        saveAvailableTicketsAsync()

        return listOf(expires, ticket)
    }

    /**
     * Remember a ticket received from a peer.
     *
     * Matches Python LXMRouter.remember_ticket() (lines 1051-1054).
     */
    private fun rememberTicket(
        sourceHashHex: String,
        ticketEntry: List<Any?>,
    ) {
        if (ticketEntry.size < 2) return
        val expires = (ticketEntry[0] as? Number)?.toLong() ?: return
        val ticket = ticketEntry[1] as? ByteArray ?: return

        outboundTickets[sourceHashHex] = Pair(expires, ticket)
        saveAvailableTicketsAsync()
    }

    /**
     * Get an outbound ticket for stamp bypass.
     *
     * Matches Python LXMRouter.get_outbound_ticket() (lines 1056-1062).
     */
    private fun getOutboundTicket(destHashHex: String): ByteArray? {
        val entry = outboundTickets[destHashHex] ?: return null
        val nowSeconds = System.currentTimeMillis() / 1000
        return if (entry.first > nowSeconds) entry.second else null
    }

    /**
     * Get valid inbound tickets for a source.
     *
     * Matches Python LXMRouter.get_inbound_tickets() (lines 1072-1083).
     */
    private fun getInboundTickets(sourceHashHex: String): List<ByteArray>? {
        val tickets = inboundTickets[sourceHashHex] ?: return null
        val nowSeconds = System.currentTimeMillis() / 1000
        val valid =
            tickets.entries
                .filter { nowSeconds < it.value }
                .map { hexToBytes(it.key) }

        return if (valid.isEmpty()) null else valid
    }

    /**
     * Extract ticket from an incoming message and remember it.
     *
     * Matches Python LXMRouter.py lines 1730-1741.
     */
    private fun extractAndRememberTicket(message: LXMessage) {
        if (!message.signatureValidated) return

        val ticketField = message.fields[LXMFConstants.FIELD_TICKET] ?: return
        val ticketEntry = ticketField as? List<*> ?: return
        if (ticketEntry.size < 2) return

        val expires = (ticketEntry[0] as? Number)?.toLong() ?: return
        val ticket = ticketEntry[1] as? ByteArray ?: return
        val nowSeconds = System.currentTimeMillis() / 1000

        if (nowSeconds < expires && ticket.size == RnsConstants.TRUNCATED_HASH_BYTES) {
            val sourceHashHex = message.sourceHash.toHexString()
            @Suppress("UNCHECKED_CAST")
            rememberTicket(sourceHashHex, ticketEntry as List<Any?>)
        }
    }

    // ===== Persistence (Phase 4) =====

    /**
     * Save outbound stamp costs to disk.
     * File: {storagePath}/lxmf/outbound_stamp_costs (msgpack)
     */
    private fun saveOutboundStampCosts() {
        val path = storagePath ?: return
        try {
            val dir = File(path, "lxmf")
            if (!dir.exists()) dir.mkdirs()

            val buffer = ByteArrayOutputStream()
            val packer = MessagePack.newDefaultPacker(buffer)

            packer.packMapHeader(outboundStampCosts.size)
            for ((hexHash, pair) in outboundStampCosts) {
                val hashBytes = hexToBytes(hexHash)
                packer.packBinaryHeader(hashBytes.size)
                packer.writePayload(hashBytes)
                packer.packArrayHeader(2)
                packer.packLong(pair.first)
                packer.packInt(pair.second)
            }
            packer.close()

            File(dir, "outbound_stamp_costs").writeBytes(buffer.toByteArray())
        } catch (e: Exception) {
            RnsLog.debug("LXMRouter") { "Could not save outbound stamp costs: ${e.message}" }
        }
    }

    private fun saveOutboundStampCostsAsync() {
        processingScope.launch {
            costFileMutex.withLock { saveOutboundStampCosts() }
        }
    }

    /**
     * Load outbound stamp costs from disk.
     */
    private fun loadOutboundStampCosts() {
        val path = storagePath ?: return
        val file = File(path, "lxmf/outbound_stamp_costs")
        if (!file.exists()) return

        try {
            val data = file.readBytes()
            val unpacker = MessagePack.newDefaultUnpacker(data)
            val mapSize = unpacker.unpackMapHeader()

            for (i in 0 until mapSize) {
                val keyLen = unpacker.unpackBinaryHeader()
                val keyBytes = ByteArray(keyLen)
                unpacker.readPayload(keyBytes)
                val hexHash = keyBytes.toHexString()

                val arrSize = unpacker.unpackArrayHeader()
                if (arrSize >= 2) {
                    val timestamp = unpacker.unpackLong()
                    val cost = unpacker.unpackInt()
                    outboundStampCosts[hexHash] = Pair(timestamp, cost)
                }
            }
            unpacker.close()

            // Clean expired entries on load
            cleanOutboundStampCosts()
        } catch (e: Exception) {
            RnsLog.debug("LXMRouter") { "Could not load outbound stamp costs: ${e.message}" }
            outboundStampCosts.clear()
        }
    }

    /**
     * Save available tickets to disk.
     * File: {storagePath}/lxmf/available_tickets (msgpack)
     */
    private fun saveAvailableTickets() {
        val path = storagePath ?: return
        try {
            val dir = File(path, "lxmf")
            if (!dir.exists()) dir.mkdirs()

            val buffer = ByteArrayOutputStream()
            val packer = MessagePack.newDefaultPacker(buffer)

            // Pack as map with 3 keys: "outbound", "inbound", "last_deliveries"
            packer.packMapHeader(3)

            // Outbound tickets
            packer.packString("outbound")
            packer.packMapHeader(outboundTickets.size)
            for ((hexHash, pair) in outboundTickets) {
                val hashBytes = hexToBytes(hexHash)
                packer.packBinaryHeader(hashBytes.size)
                packer.writePayload(hashBytes)
                packer.packArrayHeader(2)
                packer.packLong(pair.first)
                packer.packBinaryHeader(pair.second.size)
                packer.writePayload(pair.second)
            }

            // Inbound tickets
            packer.packString("inbound")
            packer.packMapHeader(inboundTickets.size)
            for ((hexHash, ticketMap) in inboundTickets) {
                val hashBytes = hexToBytes(hexHash)
                packer.packBinaryHeader(hashBytes.size)
                packer.writePayload(hashBytes)
                packer.packMapHeader(ticketMap.size)
                for ((ticketHex, expires) in ticketMap) {
                    val ticketBytes = hexToBytes(ticketHex)
                    packer.packBinaryHeader(ticketBytes.size)
                    packer.writePayload(ticketBytes)
                    packer.packArrayHeader(1)
                    packer.packLong(expires)
                }
            }

            // Last deliveries
            packer.packString("last_deliveries")
            packer.packMapHeader(lastTicketDeliveries.size)
            for ((hexHash, timestamp) in lastTicketDeliveries) {
                val hashBytes = hexToBytes(hexHash)
                packer.packBinaryHeader(hashBytes.size)
                packer.writePayload(hashBytes)
                packer.packLong(timestamp)
            }

            packer.close()
            File(dir, "available_tickets").writeBytes(buffer.toByteArray())
        } catch (e: Exception) {
            RnsLog.debug("LXMRouter") { "Could not save available tickets: ${e.message}" }
        }
    }

    private fun saveAvailableTicketsAsync() {
        processingScope.launch {
            ticketFileMutex.withLock { saveAvailableTickets() }
        }
    }

    /**
     * Load available tickets from disk.
     */
    private fun loadAvailableTickets() {
        val path = storagePath ?: return
        val file = File(path, "lxmf/available_tickets")
        if (!file.exists()) return

        try {
            val data = file.readBytes()
            val unpacker = MessagePack.newDefaultUnpacker(data)
            val mapSize = unpacker.unpackMapHeader()

            for (i in 0 until mapSize) {
                val key = unpacker.unpackString()
                when (key) {
                    "outbound" -> {
                        val outSize = unpacker.unpackMapHeader()
                        for (j in 0 until outSize) {
                            val keyLen = unpacker.unpackBinaryHeader()
                            val keyBytes = ByteArray(keyLen)
                            unpacker.readPayload(keyBytes)
                            val hexHash = keyBytes.toHexString()

                            val arrSize = unpacker.unpackArrayHeader()
                            if (arrSize >= 2) {
                                val expires = unpacker.unpackLong()
                                val ticketLen = unpacker.unpackBinaryHeader()
                                val ticket = ByteArray(ticketLen)
                                unpacker.readPayload(ticket)
                                outboundTickets[hexHash] = Pair(expires, ticket)
                            }
                        }
                    }
                    "inbound" -> {
                        val inSize = unpacker.unpackMapHeader()
                        for (j in 0 until inSize) {
                            val keyLen = unpacker.unpackBinaryHeader()
                            val keyBytes = ByteArray(keyLen)
                            unpacker.readPayload(keyBytes)
                            val hexHash = keyBytes.toHexString()

                            val ticketMapSize = unpacker.unpackMapHeader()
                            val ticketMap = ConcurrentHashMap<String, Long>()
                            for (k in 0 until ticketMapSize) {
                                val tLen = unpacker.unpackBinaryHeader()
                                val tBytes = ByteArray(tLen)
                                unpacker.readPayload(tBytes)
                                val arrSize = unpacker.unpackArrayHeader()
                                if (arrSize >= 1) {
                                    val expires = unpacker.unpackLong()
                                    ticketMap[tBytes.toHexString()] = expires
                                }
                            }
                            if (ticketMap.isNotEmpty()) {
                                inboundTickets[hexHash] = ticketMap
                            }
                        }
                    }
                    "last_deliveries" -> {
                        val ldSize = unpacker.unpackMapHeader()
                        for (j in 0 until ldSize) {
                            val keyLen = unpacker.unpackBinaryHeader()
                            val keyBytes = ByteArray(keyLen)
                            unpacker.readPayload(keyBytes)
                            val hexHash = keyBytes.toHexString()
                            val timestamp = unpacker.unpackLong()
                            lastTicketDeliveries[hexHash] = timestamp
                        }
                    }
                    else -> unpacker.skipValue()
                }
            }
            unpacker.close()
        } catch (e: Exception) {
            RnsLog.debug("LXMRouter") { "Could not load available tickets: ${e.message}" }
            outboundTickets.clear()
            inboundTickets.clear()
            lastTicketDeliveries.clear()
        }
    }

    /**
     * Save locally delivered transient IDs to disk.
     * File: {storagePath}/lxmf/local_deliveries (msgpack)
     */
    private fun saveTransientIds() {
        val path = storagePath ?: return
        if (locallyDeliveredTransientIds.isEmpty()) return

        try {
            val dir = File(path, "lxmf")
            if (!dir.exists()) dir.mkdirs()

            val buffer = ByteArrayOutputStream()
            val packer = MessagePack.newDefaultPacker(buffer)

            packer.packMapHeader(locallyDeliveredTransientIds.size)
            for ((hexHash, timestamp) in locallyDeliveredTransientIds) {
                val hashBytes = hexToBytes(hexHash)
                packer.packBinaryHeader(hashBytes.size)
                packer.writePayload(hashBytes)
                packer.packLong(timestamp)
            }
            packer.close()

            File(dir, "local_deliveries").writeBytes(buffer.toByteArray())
        } catch (e: Exception) {
            RnsLog.debug("LXMRouter") { "Could not save transient IDs: ${e.message}" }
        }
    }

    private fun saveTransientIdsAsync() {
        processingScope.launch {
            transientIdFileMutex.withLock { saveTransientIds() }
        }
    }

    /**
     * Load locally delivered transient IDs from disk.
     */
    private fun loadTransientIds() {
        val path = storagePath ?: return
        val file = File(path, "lxmf/local_deliveries")
        if (!file.exists()) return

        try {
            val data = file.readBytes()
            val unpacker = MessagePack.newDefaultUnpacker(data)
            val mapSize = unpacker.unpackMapHeader()

            for (i in 0 until mapSize) {
                val keyLen = unpacker.unpackBinaryHeader()
                val keyBytes = ByteArray(keyLen)
                unpacker.readPayload(keyBytes)
                val hexHash = keyBytes.toHexString()
                val timestamp = unpacker.unpackLong()
                locallyDeliveredTransientIds[hexHash] = timestamp
            }
            unpacker.close()
        } catch (e: Exception) {
            RnsLog.debug("LXMRouter") { "Could not load transient IDs: ${e.message}" }
            locallyDeliveredTransientIds.clear()
        }
    }

    // ===== Access Control (Phase 5) =====

    /**
     * Set whether authentication is required for message delivery.
     */
    fun setAuthentication(required: Boolean) {
        authRequired = required
    }

    /**
     * Check if authentication is required.
     */
    fun requiresAuthentication(): Boolean = authRequired

    /**
     * Add an identity hash to the allowed list.
     *
     * @param identityHash Truncated identity hash (16 bytes)
     * @throws IllegalArgumentException if hash length is wrong
     */
    fun allow(identityHash: ByteArray) {
        require(identityHash.size == RnsConstants.TRUNCATED_HASH_BYTES) {
            "Allowed identity hash must be ${RnsConstants.TRUNCATED_HASH_BYTES} bytes"
        }
        if (allowedList.none { it.contentEquals(identityHash) }) {
            allowedList.add(identityHash)
        }
    }

    /**
     * Remove an identity hash from the allowed list.
     */
    fun disallow(identityHash: ByteArray) {
        require(identityHash.size == RnsConstants.TRUNCATED_HASH_BYTES) {
            "Disallowed identity hash must be ${RnsConstants.TRUNCATED_HASH_BYTES} bytes"
        }
        allowedList.removeAll { it.contentEquals(identityHash) }
    }

    /**
     * Add a destination hash to the ignored list.
     * Messages from ignored destinations are silently dropped.
     */
    fun ignoreDestination(destinationHash: ByteArray) {
        if (ignoredList.none { it.contentEquals(destinationHash) }) {
            ignoredList.add(destinationHash)
        }
    }

    /**
     * Remove a destination hash from the ignored list.
     */
    fun unignoreDestination(destinationHash: ByteArray) {
        ignoredList.removeAll { it.contentEquals(destinationHash) }
    }

    /**
     * Add a destination hash to the prioritised list.
     *
     * @param destinationHash Truncated destination hash (16 bytes)
     * @throws IllegalArgumentException if hash length is wrong
     */
    fun prioritise(destinationHash: ByteArray) {
        require(destinationHash.size == RnsConstants.TRUNCATED_HASH_BYTES) {
            "Prioritised destination hash must be ${RnsConstants.TRUNCATED_HASH_BYTES} bytes"
        }
        if (prioritisedList.none { it.contentEquals(destinationHash) }) {
            prioritisedList.add(destinationHash)
        }
    }

    /**
     * Remove a destination hash from the prioritised list.
     */
    fun unprioritise(destinationHash: ByteArray) {
        require(destinationHash.size == RnsConstants.TRUNCATED_HASH_BYTES) {
            "Prioritised destination hash must be ${RnsConstants.TRUNCATED_HASH_BYTES} bytes"
        }
        prioritisedList.removeAll { it.contentEquals(destinationHash) }
    }

    /**
     * Check if an identity is allowed to deliver messages.
     */
    fun identityAllowed(identity: Identity): Boolean =
        if (authRequired) {
            allowedList.any { it.contentEquals(identity.hash) }
        } else {
            true
        }

    // ===== Cleanup Jobs (Phase 6) =====

    /**
     * Clean expired entries from transient ID caches.
     *
     * Matches Python LXMRouter.clean_transient_id_caches() (lines 955-975):
     * Removes entries older than MESSAGE_EXPIRY * 6 (180 days).
     */
    private fun cleanTransientIdCaches() {
        val nowSeconds = System.currentTimeMillis() / 1000
        val expiryThreshold = LXMFConstants.MESSAGE_EXPIRY * 6

        val removed =
            locallyDeliveredTransientIds.entries.removeIf { (_, timestamp) ->
                nowSeconds > timestamp + expiryThreshold
            }
        if (removed) {
            saveTransientIdsAsync()
        }
    }

    /**
     * Clean expired outbound stamp costs.
     *
     * Matches Python LXMRouter.clean_outbound_stamp_costs() (lines 1217-1230):
     * Removes entries older than STAMP_COST_EXPIRY (45 days).
     */
    private fun cleanOutboundStampCosts() {
        try {
            val nowSeconds = System.currentTimeMillis() / 1000
            val removed =
                outboundStampCosts.entries.removeIf { (_, pair) ->
                    nowSeconds > pair.first + LXMFConstants.STAMP_COST_EXPIRY
                }
            if (removed) {
                saveOutboundStampCostsAsync()
            }
        } catch (e: Exception) {
            RnsLog.error("LXMRouter") { "Error cleaning outbound stamp costs: ${e.message}" }
        }
    }

    /**
     * Clean expired available tickets.
     *
     * Matches Python LXMRouter.clean_available_tickets() (lines 1245-1271):
     * - Outbound: remove expired tickets
     * - Inbound: remove expired tickets + TICKET_GRACE
     */
    private fun cleanAvailableTickets() {
        try {
            val nowSeconds = System.currentTimeMillis() / 1000

            // Clean outbound tickets
            outboundTickets.entries.removeIf { (_, pair) ->
                nowSeconds > pair.first
            }

            // Clean inbound tickets
            for ((_, ticketMap) in inboundTickets) {
                ticketMap.entries.removeIf { (_, expires) ->
                    nowSeconds > expires + LXMFConstants.TICKET_GRACE
                }
            }

            // Remove empty inbound ticket maps
            inboundTickets.entries.removeIf { (_, ticketMap) ->
                ticketMap.isEmpty()
            }
        } catch (e: Exception) {
            RnsLog.error("LXMRouter") { "Error cleaning available tickets: ${e.message}" }
        }
    }

    // ===== PAPER Delivery (Phase 6) =====

    /**
     * Ingest a paper message from an lxm:// URI.
     *
     * Matches Python LXMRouter.ingest_lxm_uri() (lines 2358-2378):
     * 1. Validate URI prefix
     * 2. Strip prefix and decode base64url
     * 3. Process as paper message (no stamp enforcement)
     *
     * @param uri The lxm:// URI string
     * @return True if the message was successfully ingested
     */
    fun ingestLxmUri(uri: String): Boolean {
        try {
            val schema = "${LXMFConstants.URI_SCHEMA}://"
            if (!uri.lowercase().startsWith(schema)) {
                RnsLog.error("LXMRouter") { "Cannot ingest LXM, invalid URI provided" }
                return false
            }

            // Decode: remove protocol, remove slashes, add padding, base64url-decode
            val encoded =
                uri
                    .removePrefix(schema)
                    .replace(LXMFConstants.URI_SCHEMA + "://", "")
                    .replace("/", "") + "=="

            val lxmfData = Base64.getUrlDecoder().decode(encoded)
            val transientId = Hashes.fullHash(lxmfData)
            val transientIdHex = transientId.toHexString()

            // Check for duplicates
            if (locallyDeliveredTransientIds.containsKey(transientIdHex)) {
                RnsLog.debug("LXMRouter") { "Paper message already delivered, ignoring duplicate" }
                return false
            }

            // Process as inbound delivery (no stamp enforcement for paper messages)
            processInboundDelivery(lxmfData, DeliveryMethod.PAPER)

            RnsLog.debug("LXMRouter") { "Ingested paper message with transient ID ${transientIdHex.take(12)}" }
            return true
        } catch (e: Exception) {
            RnsLog.error("LXMRouter") { "Error decoding URI-encoded LXMF message: ${e.message}" }
            return false
        }
    }

    // ===== Utility =====

    /**
     * Convert hex string to byte array.
     */
    private fun hexToBytes(hex: String): ByteArray {
        val len = hex.length
        val data = ByteArray(len / 2)
        var i = 0
        while (i < len) {
            data[i / 2] = ((Character.digit(hex[i], 16) shl 4) + Character.digit(hex[i + 1], 16)).toByte()
            i += 2
        }
        return data
    }

    /**
     * Earlier LXMF-kt revisions wrote per-destination ratchet blobs to
     * `$path/lxmf/ratchets/<hash>` with no suffix, diverging from Python
     * LXMF's `<hash>.ratchets`. This renames any legacy file for the current
     * destination to the suffixed filename on next-boot, so an upgrading
     * consumer keeps its ratchet state instead of silently starting fresh.
     *
     * Scoped to the specific destination we're enabling so we touch the
     * filesystem only when actually preparing ratchets for that destination.
     * Best-effort: failure here just means the destination regenerates a
     * ratchet, which is forward-secrecy safe.
     */
    private fun migrateLegacyRatchetFiles(
        ratchetsDir: File,
        hexHash: String,
    ) {
        if (!ratchetsDir.exists() || !ratchetsDir.isDirectory) return
        val legacy = File(ratchetsDir, hexHash)
        val modern = File(ratchetsDir, "$hexHash.ratchets")
        if (!legacy.isFile || modern.exists()) return
        // renameTo returns false on failure (e.g. cross-filesystem move on
        // Android) rather than throwing, so no try/catch is needed. Discarded
        // intentionally — if the rename doesn't stick, Destination will just
        // generate a fresh ratchet, which is forward-secrecy safe.
        legacy.renameTo(modern)
    }

    // ===== Propagation Node Persistence =====

    private fun getPropagationNodesFile(): java.io.File? {
        val path = storagePath ?: return null
        val dir = java.io.File(path, "lxmf")
        if (!dir.exists()) dir.mkdirs()
        return java.io.File(dir, "propagation_nodes")
    }

    private fun savePropagationNodes() {
        val file = getPropagationNodesFile() ?: return
        try {
            val buffer = java.io.ByteArrayOutputStream()
            val packer =
                org.msgpack.core.MessagePack
                    .newDefaultPacker(buffer)
            val nodes = propagationNodes.values.toList()
            packer.packMapHeader(nodes.size)
            for (node in nodes) {
                packer.packString(node.hexHash)
                packer.packMapHeader(8)
                packer.packString("dest_hash")
                packer.packBinaryHeader(node.destHash.size)
                packer.writePayload(node.destHash)
                packer.packString("identity_pub")
                packer.packBinaryHeader(node.identity.getPublicKey().size)
                packer.writePayload(node.identity.getPublicKey())
                packer.packString("display_name")
                if (node.displayName != null) packer.packString(node.displayName!!) else packer.packNil()
                packer.packString("timebase")
                packer.packLong(node.timebase)
                packer.packString("is_active")
                packer.packBoolean(node.isActive)
                packer.packString("per_transfer_limit")
                packer.packInt(node.perTransferLimit)
                packer.packString("per_sync_limit")
                packer.packInt(node.perSyncLimit)
                packer.packString("stamp_cost")
                packer.packInt(node.stampCost)
            }
            packer.close()
            file.writeBytes(buffer.toByteArray())
            RnsLog.debug("LXMRouter") { "Saved ${nodes.size} propagation nodes to ${file.name}" }
        } catch (e: Exception) {
            RnsLog.error("LXMRouter") { "Error saving propagation nodes: ${e.message}" }
        }
    }

    private fun loadPropagationNodes() {
        val file = getPropagationNodesFile() ?: return
        if (!file.exists()) return
        try {
            val data = file.readBytes()
            val unpacker =
                org.msgpack.core.MessagePack
                    .newDefaultUnpacker(data)
            val mapSize = unpacker.unpackMapHeader()
            var loaded = 0
            for (i in 0 until mapSize) {
                val hexHash = unpacker.unpackString()
                val fieldCount = unpacker.unpackMapHeader()
                var destHash: ByteArray? = null
                var pubKey: ByteArray? = null
                var displayName: String? = null
                var timebase = 0L
                var isActive = true
                var perTransferLimit = LXMFConstants.PROPAGATION_LIMIT_KB
                var perSyncLimit = LXMFConstants.SYNC_LIMIT_KB
                var stampCost = LXMFConstants.PROPAGATION_COST

                for (j in 0 until fieldCount) {
                    val key = unpacker.unpackString()
                    when (key) {
                        "dest_hash" -> {
                            val len = unpacker.unpackBinaryHeader()
                            destHash = ByteArray(len)
                            unpacker.readPayload(destHash)
                        }
                        "identity_pub" -> {
                            val len = unpacker.unpackBinaryHeader()
                            pubKey = ByteArray(len)
                            unpacker.readPayload(pubKey)
                        }
                        "display_name" -> displayName = if (!unpacker.tryUnpackNil()) unpacker.unpackString() else null
                        "timebase" -> timebase = unpackAsLong(unpacker)
                        "is_active" -> isActive = unpacker.unpackBoolean()
                        "per_transfer_limit" -> perTransferLimit = unpackAsInt(unpacker)
                        "per_sync_limit" -> perSyncLimit = unpackAsInt(unpacker)
                        "stamp_cost" -> stampCost = unpackAsInt(unpacker)
                        else -> unpacker.skipValue()
                    }
                }

                if (destHash != null && pubKey != null) {
                    val identity = Identity.fromPublicKey(pubKey)
                    val node =
                        PropagationNode(
                            destHash = destHash,
                            identity = identity,
                            displayName = displayName,
                            timebase = timebase,
                            isActive = isActive,
                            perTransferLimit = perTransferLimit,
                            perSyncLimit = perSyncLimit,
                            stampCost = stampCost,
                        )
                    propagationNodes[hexHash] = node
                    loaded++
                }
            }
            unpacker.close()
            RnsLog.debug("LXMRouter") { "Loaded $loaded propagation nodes from ${file.name}" }
        } catch (e: Exception) {
            RnsLog.error("LXMRouter") { "Error loading propagation nodes: ${e.message}" }
        }
    }

    /** Unpack a msgpack value as Int, tolerating float encoding. Matches Python's int() coercion. */
    private fun unpackAsInt(unpacker: org.msgpack.core.MessageUnpacker): Int =
        when (unpacker.nextFormat.valueType) {
            org.msgpack.value.ValueType.INTEGER -> unpacker.unpackInt()
            org.msgpack.value.ValueType.FLOAT -> unpacker.unpackDouble().toInt()
            else -> {
                unpacker.skipValue()
                0
            }
        }

    /** Unpack a msgpack value as Long, tolerating float encoding. */
    private fun unpackAsLong(unpacker: org.msgpack.core.MessageUnpacker): Long =
        when (unpacker.nextFormat.valueType) {
            org.msgpack.value.ValueType.INTEGER -> unpacker.unpackLong()
            org.msgpack.value.ValueType.FLOAT -> unpacker.unpackDouble().toLong()
            else -> {
                unpacker.skipValue()
                0L
            }
        }
}
