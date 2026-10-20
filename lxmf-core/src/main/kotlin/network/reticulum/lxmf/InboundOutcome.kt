package network.reticulum.lxmf

/**
 * What became of an inbound message in [LXMRouter].
 *
 * Only [RefusedByPolicy] withholds the delivery proof. A duplicate and a message the router could
 * not read are still proven, as in Python LXMF, so a sender whose first proof was lost is not left
 * retrying.
 */
internal sealed interface InboundOutcome {

    /** Whether the packet that delivered the message is proven to its sender. */
    val proves: Boolean
        get() = this !is RefusedByPolicy

    /** Delivered to the host. */
    class Admitted(val message: LXMessage) : InboundOutcome

    /** Delivered once already. Dropped. */
    data object Duplicate : InboundOutcome

    /** Refused by [DeliveryPolicy]. Dropped without a proof. */
    data object RefusedByPolicy : InboundOutcome

    /**
     * Dropped for another reason: too large, unreadable, a bad signature, a source on the ignore
     * list, or a bad stamp.
     */
    data object Rejected : InboundOutcome
}
