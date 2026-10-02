# Peer exchange and dial recovery

Peer exchange is an unsigned discovery hint. It cannot establish a BLS identity, change
committee membership, grant Closed admission, or reset dial failures. Every swarm applies
the same admission snapshot used by its connection hooks.

The wire format has no authenticated remote Open/Closed assertion. The conservative policy
therefore reserves committee hints for verified committee recipients. An observer does not
learn committee endpoints through exchange, even when another peer keeps advertising them.
Open hubs and observers outside the committee window remain discoverable. An operator can
still configure direct trusted/bootstrap dials, and committee records remain resolvable
through authenticated Kademlia discovery. This avoids inventing remote admission authority
from an unsigned BLS key or an address. A future signed reachability protocol could permit
more selective committee-to-observer exchange.

On input, previous/current/next membership comes from authoritative local state. A committee
hint additionally needs a signature-verified record binding its advertised network identity
to that BLS key. Its dial addresses come from that accepted record, rather than the unsigned
hint. Advertising a known validator under a different BLS key does not evade the filter.
Closed additionally excludes identities outside the snapshot's authorized set. Grace/Open
fallback retains ordinary hub and observer discovery; it does not turn hints into grants.

On output, the authenticated recipient determines whether committee hints are appropriate.
Calls without a recipient use the conservative observer view. Filtering happens before
address cloning, collection or reservoir sampling. The existing exchange, per-peer address
and discovery-set caps remain in force.

Actual transport failures, missing addresses, identity mismatches and dial timeouts start
an exponential cooldown: 1, 2, 4 seconds, up to 120 seconds. Manager requests, discovery
selection and Kademlia's pending outbound hook all consult the same memory. A rejected
cooldown request does not extend it or penalize reputation. Discovery eviction does not
erase failures.

Each swarm retains at most its configured disconnected-peer budget (at least one) of
failed identities. At capacity, new identities share a finite 120-second overflow cooldown
instead of evicting live failure memory. Failures of tracked identities do not extend that
deadline, so saturation cannot permanently defer an unchanged untracked endpoint.
An accepted authoritative mapping may evict the oldest failure to reserve one retry
slot, while preserving the same hard cap. Unsigned hints cannot reserve slots. Entries expire
after 15 idle minutes. Successful authenticated inbound or outbound
connections clear their entry. An accepted change to a verified BLS-to-network-identity or
endpoint mapping clears both the old and new identities and queues an immediate permitted
recovery attempt. A newer timestamp with the same endpoint does not reset the delay, and
committee lease renewals do not reset it either. Stable endpoints remain retryable after
at most 120 seconds when connectivity returns.

Committee dial tasks log the first failure and distinct actionable error classes, with at
most eight warnings per peer and swarm per minute. Repeated failures contribute to the next
summary. Cooldown and already-dialing replies do not create warnings or count as transport
failures. Successful connection produces one recovery summary with the outage failure
count. Real failures outside the logging interval remain visible as periodic summaries.
All handles and tasks for a swarm share the same budget across epochs. Logging memory uses
the disconnected-peer capacity; additional identities share a bounded overflow budget.
Recovery consumes a tracked identity's outage once. Overflow identities cannot produce an
attributed recovery count until they obtain a tracked slot.
