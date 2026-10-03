package peerwire

// BufferCap exposes the decoder's buffer capacity so tests can check that
// allocation stays within Limits.MaxBuffered.
func BufferCap(d *Decoder) int { return cap(d.buf) }
