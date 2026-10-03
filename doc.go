// Package aggregator sums per-proxy HAProxy stick-table contributions
// received over the peers protocol and publishes the aggregate back to
// ordinary stick tables for local enforcement.
//
// The design contract and phased plan live in docs/plans.
package aggregator
