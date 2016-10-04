package io.omega;

/**
 * Created by arrawatia on 10/3/16.
 */
public class ConnectionQuotas {
}
class ConnectionQuotas(val defaultMax: Int, overrideQuotas: Map[String, Int]) {

private val overrides = overrideQuotas.map { case (host, count) => (InetAddress.getByName(host), count) }
private val counts = mutable.Map[InetAddress, Int]()

    def inc(address: InetAddress) {
    counts.synchronized {
    val count = counts.getOrElseUpdate(address, 0)
    counts.put(address, count + 1)
    val max = overrides.getOrElse(address, defaultMax)
    if (count >= max)
    throw new TooManyConnectionsException(address, max)
    }
    }

    def dec(address: InetAddress) {
    counts.synchronized {
    val count = counts.getOrElse(address,
    throw new IllegalArgumentException(s"Attempted to decrease connection count for address with no connections, address: $address"))
    if (count == 1)
    counts.remove(address)
    else
    counts.put(address, count - 1)
    }
    }

    def get(address: InetAddress): Int = counts.synchronized {
    counts.getOrElse(address, 0)
    }

    }
