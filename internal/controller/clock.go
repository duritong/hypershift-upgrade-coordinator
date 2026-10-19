package controller

import "time"

// Clock supplies policy wall time, not network deadlines or cancellation.
type Clock interface {
	Now() time.Time
}

func (r *HostedClusterUpgradeReconciler) now() time.Time {
	if r.Clock != nil {
		return r.Clock.Now()
	}
	return time.Now()
}
