package io.omega;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class ConnectionQuotas {

    private final int defaultMax;
    private final Map<String, Integer> overrideQuotas;
    private Map<InetAddress, Integer> overrides;

    public ConnectionQuotas(int defaultMax, Map<String, Integer> overrideQuotas) {
        this.defaultMax = defaultMax;
        this.overrideQuotas = overrideQuotas;
        this.overrides = overrideQuotas.entrySet().stream()
                .collect(Collectors.toMap(
                        e -> {
                            try {
                                return InetAddress.getByName(e.getKey());
                            } catch (UnknownHostException e1) {
                                return null;
                            }
                        },
                        e -> e.getValue()
                ));
    }

    private Map<InetAddress, Integer> counts = new HashMap<InetAddress, Integer>();

    public void inc(InetAddress address) {
        synchronized (counts) {
            Integer count = counts.getOrDefault(address, 0);
            counts.put(address, count + 1);
            Integer max = overrides.getOrDefault(address, this.defaultMax);
            if (count >= max) {
                throw new TooManyConnectionsException(address, max);
            }
        }
    }

    public void dec(InetAddress address) {
        synchronized (counts) {

            if (!counts.containsKey(address))
                throw new IllegalArgumentException("Attempted to decrease connection count for " +
                        "address with no connections, address:" + address);
            int count = counts.get(address);
            if (count == 1) {
                counts.remove(address);
            } else {
                counts.put(address, count - 1);
            }
        }
    }

    public int get(InetAddress address) {
        synchronized (counts) {
            return counts.getOrDefault(address, 0);
        }
    }

}
