package network.reticulum.lxmf

/**
 * How long to wait before the next attempt at a message that has not gone through yet.
 *
 * Doubling from two seconds and stopping at five minutes. The shape matters more than the numbers:
 * the early retries are close together because most failures are transient — a carrier that came up
 * a moment too late, a peer whose radio was busy — and those are fixed by trying again almost
 * immediately. The later ones spread out because a message still undelivered after several minutes
 * is waiting on something that will not be fixed by asking faster, and a device on battery should
 * not spend the next hour asking at two-second intervals.
 *
 * This is deliberately not the constant ten-second wait Python uses. A constant wait is both too
 * slow at the start (ten seconds is an age when the peer is one hop away and simply was not
 * listening yet) and too fast at the end (a peer that has been away for an hour is still being
 * polled every ten seconds). This curve is standard across the clients we ship and they must
 * agree, because a message crossing between them is retried by whichever side sent it.
 *
 * Pure and separate from the router so the curve can be asserted directly rather than inferred from
 * a scheduler's behaviour under a test clock.
 */
internal object RetryBackoff {

    /** First wait, and the factor each further step multiplies by. */
    const val BASE_MILLIS = 2_000L

    /** The longest this will ever ask anybody to wait. */
    const val CAP_MILLIS = 300_000L

    /**
     * The wait after [step] failed attempts, in milliseconds. Step 0 is the first retry.
     *
     * Negative steps are treated as zero, and the doubling is clamped well before it could overflow
     * — the cap has already been reached by step 8, so anything beyond that is the same number.
     */
    fun afterStep(step: Int): Long {
        val clamped = step.coerceIn(0, MAX_SHIFT)

        return minOf(CAP_MILLIS, BASE_MILLIS shl clamped)
    }

    /**
     * Bounds the shift rather than the result. Purely defensive: a step this large means a message
     * has been retried sixteen times and the cap decided the answer several steps ago, but shifting
     * by 64 is undefined-ish rather than merely large, so it never gets the chance.
     */
    private const val MAX_SHIFT = 16
}
