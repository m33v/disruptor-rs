pub trait WaitStrategy: Copy + Send {
	/// The wait strategy will wait for the sequence id being available.
	fn wait_for(&self, sequence: i64);
}

/// Busy spin wait strategy. Lowest possible latency.
#[derive(Copy, Clone)]
pub struct BusySpin;

impl WaitStrategy for BusySpin {
	#[inline]
	fn wait_for(&self, _sequence: i64) {
		// Do nothing, true busy spin.
	}
}