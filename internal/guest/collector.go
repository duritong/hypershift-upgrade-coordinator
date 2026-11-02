package guest

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/url"
	"reflect"
	"time"

	configv1 "github.com/openshift/api/config/v1"
	"k8s.io/client-go/rest"
	strictjson "sigs.k8s.io/json"
)

const apiPath = "/apis/config.openshift.io/v1/"

type Reader struct {
	client *rest.RESTClient
	target Target
	now    func() time.Time
}

// Collect uses real context deadlines regardless of the injected policy clock.
// Failed reads never return a partially usable snapshot. The byte budget counts
// ALL response bytes (including both CV reads) before typed JSON decoding.
func (r *Reader) Collect(ctx context.Context) (Snapshot, error) {
	ctx, cancel := context.WithTimeout(ctx, CollectionTimeout)
	defer cancel()
	s := Snapshot{Target: r.target, StartedAt: r.now()}
	if !s.StartedAt.Before(r.target.CredentialExpiresAt) {
		return Snapshot{}, &Failure{Unavailable, "CredentialExpired"}
	}
	remaining := int64(MaxBytes)
	var before, after configv1.ClusterVersion
	if err := r.get(ctx, "clusterversions/version", nil, &before, &remaining); err != nil {
		return Snapshot{}, err
	}
	if before.Name != "version" || before.UID != r.target.CVUID || before.Spec.ClusterID != r.target.ClusterID {
		return Snapshot{}, &Failure{IdentityMismatch, "ClusterVersionIdentity"}
	}
	if before.Kind != "ClusterVersion" || before.APIVersion != "config.openshift.io/v1" || before.Generation <= 0 || before.ResourceVersion == "" || before.Namespace != "" {
		return Snapshot{}, &Failure{UnsupportedEvidence, "MalformedClusterVersion"}
	}
	seenTokens := map[string]bool{}
	next := ""
	totalHint := int64(-1)
	for {
		var page configv1.ClusterOperatorList
		q := url.Values{"limit": {"256"}}
		if next != "" {
			q.Set("continue", next)
		}
		if err := r.get(ctx, "clusteroperators", q, &page, &remaining); err != nil {
			return Snapshot{}, err
		}
		if page.Kind != "ClusterOperatorList" || page.APIVersion != "config.openshift.io/v1" || page.ResourceVersion == "" || page.Items == nil {
			return Snapshot{}, &Failure{UnsupportedEvidence, "MalformedOperatorList"}
		}
		if s.ListRevision != "" && s.ListRevision != page.ResourceVersion {
			return Snapshot{}, &Failure{Unavailable, "ListRevisionConflict"}
		}
		s.ListRevision = page.ResourceVersion
		if len(s.Operators)+len(page.Items) > MaxOperators {
			return Snapshot{}, &Failure{UnsupportedEvidence, "OperatorLimit"}
		}
		s.Operators = append(s.Operators, page.Items...)
		next = page.Continue
		if page.RemainingItemCount != nil {
			count := *page.RemainingItemCount
			if count < 0 || count > int64(MaxOperators-len(s.Operators)) {
				return Snapshot{}, &Failure{UnsupportedEvidence, "InvalidRemainingCount"}
			}
			total := int64(len(s.Operators)) + count
			if totalHint >= 0 && totalHint != total {
				return Snapshot{}, &Failure{UnsupportedEvidence, "PartialOperatorList"}
			}
			totalHint = total
		}
		if totalHint >= 0 && (totalHint < int64(len(s.Operators)) || (next == "" && totalHint != int64(len(s.Operators))) || (next != "" && totalHint == int64(len(s.Operators)))) {
			return Snapshot{}, &Failure{UnsupportedEvidence, "PartialOperatorList"}
		}
		if next == "" {
			break
		}
		if seenTokens[next] || len(page.Items) == 0 || len(s.Operators) == MaxOperators {
			return Snapshot{}, &Failure{UnsupportedEvidence, "InvalidPagination"}
		}
		seenTokens[next] = true
	}
	if len(s.Operators) == 0 {
		return Snapshot{}, &Failure{UnsupportedEvidence, "EmptyOperatorList"}
	}

	virt, err := r.collectVirtualization(ctx, &remaining)
	if err != nil {
		return Snapshot{}, err
	}
	s.Virtualization = virt

	if err := r.get(ctx, "clusterversions/version", nil, &after, &remaining); err != nil {
		return Snapshot{}, err
	}
	// ResourceVersion is a conservative conflict fence, not a heartbeat. Also
	// compare relevant fields in case a malformed endpoint reuses an RV.
	if before.TypeMeta != after.TypeMeta || before.Namespace != after.Namespace || before.Name != after.Name || before.UID != after.UID || before.ResourceVersion != after.ResourceVersion || before.Generation != after.Generation || before.Spec.ClusterID != after.Spec.ClusterID || !reflect.DeepEqual(before.Spec.DesiredUpdate, after.Spec.DesiredUpdate) || !reflect.DeepEqual(before.Status, after.Status) {
		return Snapshot{}, &Failure{Unavailable, "ClusterVersionConflict"}
	}
	s.Version = after
	s.CompletedAt = r.now()
	if err := ctx.Err(); err != nil {
		return Snapshot{}, contextFailure(err)
	}
	if !s.CompletedAt.Before(r.target.CredentialExpiresAt) {
		return Snapshot{}, &Failure{Unavailable, "CredentialExpired"}
	}
	s.complete = true
	return s, nil
}

func (r *Reader) get(ctx context.Context, resource string, query url.Values, out any, remaining *int64) error {
	_, err := r.getAbs(ctx, apiPath+resource, query, out, remaining, false)
	return err
}

func (r *Reader) getAbs(ctx context.Context, absPath string, query url.Values, out any, remaining *int64, allowNotFound bool) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, RequestTimeout)
	defer cancel()
	budget := &byteBudget{remaining: remaining}
	ctx = context.WithValue(ctx, budgetKey{}, budget)
	req := r.client.Get().AbsPath(absPath).SetHeader("Accept", "application/json").MaxRetries(0)
	for key, values := range query {
		for _, value := range values {
			req.Param(key, value)
		}
	}
	bodyStream, err := req.Stream(ctx)
	if err != nil {
		if budget.exceeded {
			return false, &Failure{UnsupportedEvidence, "ByteLimit"}
		}
		if budget.status == http.StatusNotFound && allowNotFound {
			return false, nil
		}
		if budget.status == http.StatusUnauthorized || budget.status == http.StatusForbidden {
			return false, &Failure{AccessDenied, "ObserverDenied"}
		}
		if budget.status != 0 && budget.status != http.StatusOK {
			return false, &Failure{Unavailable, "GuestHTTPError"}
		}
		return false, contextFailure(ctx.Err())
	}
	defer bodyStream.Close()
	body, err := io.ReadAll(bodyStream)
	if budget.exceeded {
		return false, &Failure{UnsupportedEvidence, "ByteLimit"}
	}
	if err != nil {
		return false, contextFailure(ctx.Err())
	}
	strict, err := strictjson.UnmarshalStrict(body, out, strictjson.DisallowDuplicateFields)
	if err != nil || len(strict) != 0 {
		return false, &Failure{UnsupportedEvidence, "MalformedJSON"}
	}
	return true, nil
}

type budgetKey struct{}
type byteBudget struct {
	remaining *int64
	exceeded  bool
	status    int
}
type boundedTransport struct{ base http.RoundTripper }
type boundedBody struct {
	io.ReadCloser
	budget *byteBudget
}

// This wrapper also bounds client-go's decoding of non-2xx Status responses.
func (t boundedTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	b, ok := req.Context().Value(budgetKey{}).(*byteBudget)
	if !ok {
		return nil, &Failure{Unavailable, "MissingCollectionBudget"}
	}
	resp, err := t.base.RoundTrip(req)
	if err != nil {
		return nil, err
	}
	b.status = resp.StatusCode
	if resp.ContentLength > *b.remaining {
		resp.Body.Close()
		b.exceeded = true
		return nil, &Failure{UnsupportedEvidence, "ByteLimit"}
	}
	resp.Body = &boundedBody{resp.Body, b}
	if resp.StatusCode != http.StatusOK {
		// Do not let client-go decode or verbose-log an untrusted error body.
		// Partial (206) and redirect responses are not complete API evidence.
		_, err := io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		if err != nil {
			return nil, &Failure{Unavailable, "GuestHTTPBodyError"}
		}
		return nil, &Failure{Unavailable, "GuestHTTPError"}
	}
	return resp, nil
}

func (b *boundedBody) Read(p []byte) (int, error) {
	if b.budget.exceeded {
		return 0, &Failure{UnsupportedEvidence, "ByteLimit"}
	}
	if int64(len(p)) > *b.budget.remaining+1 {
		p = p[:int(*b.budget.remaining+1)]
	}
	n, err := b.ReadCloser.Read(p)
	*b.budget.remaining -= int64(n)
	if *b.budget.remaining < 0 {
		b.budget.exceeded = true
		return 0, &Failure{UnsupportedEvidence, "ByteLimit"}
	}
	return n, err
}

func contextFailure(err error) error {
	if errors.Is(err, context.Canceled) {
		return &Failure{Unavailable, "Canceled"}
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return &Failure{Unavailable, "DeadlineExceeded"}
	}
	return &Failure{Unavailable, "GuestTransportError"}
}
