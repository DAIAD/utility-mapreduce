package eu.daiad.mapreduce.hbase.model;

/**
 * Represents a single smart water meter reading.
 */
public class MeterData {

	private long utcTimestamp;

	private byte[] serialHash;

	private float volume;

	private float difference;

	public MeterData(long utcTimestamp, byte[] serialHash, float volume, float difference) {
		this.utcTimestamp = utcTimestamp;
		this.serialHash = serialHash;
		this.volume = volume;
		this.difference = difference;
	}

	/**
	 * Unix timestamp. It must be converted to the appropriate time zone before aggregation.
	 *
	 * @return the timestamp.
	 */
	public long getUtcTimestamp() {
		return utcTimestamp;
	}

	/**
	 * The MD5 hash of the smart water meter serial number.
	 *
	 * @return the hash bytes.
	 */
	public byte[] getSerialHash() {
		return serialHash;
	}

	/**
	 * The current SWM value.
	 *
	 * @return the total consumption until {@code timestamp}.
	 */
	public float getVolume() {
	    return volume;
	}
	/**
	 * The difference since the previous reading.
	 *
	 * @return the volume difference.
	 */
	public float getDifference() {
		return difference;
	}

}
