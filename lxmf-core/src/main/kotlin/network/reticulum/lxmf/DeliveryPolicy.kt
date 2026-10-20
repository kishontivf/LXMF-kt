package network.reticulum.lxmf

/**
 * Decides whether [LXMRouter] delivers an inbound message.
 *
 * The host installs one as [LXMRouter.deliveryPolicy]. The router asks it after the message is
 * unpacked and its signature checked, and before the duplicate check, the delivery and the proof.
 * A refused message is not proven, not delivered and not remembered. So the same message is
 * accepted once the policy allows its sender. Without a policy every message is delivered.
 *
 * The router asks on the thread that received the message, once per inbound message. Answer from
 * memory. Do not block on I/O here.
 *
 * Public because the host supplies it. LXMF-swift has the same hook, `LXDeliveryPolicy`.
 */
fun interface DeliveryPolicy {

    /**
     * Tells whether a message may be delivered.
     *
     * @param sourceHash the sender's LXMF delivery destination hash, 16 bytes. The router has
     *   verified it against the sender's signature when it knows the sender's identity.
     * @param fields the message's fields, by LXMF field id. Empty when the message carries none.
     * @return true to deliver the message, false to refuse it.
     */
    fun admits(sourceHash: ByteArray, fields: Map<Int, Any>): Boolean
}
