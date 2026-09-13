// Copyright 2026 Woodpecker Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package docker

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/cenkalti/backoff/v7"
	"github.com/containerd/errdefs"
	"github.com/moby/moby/api/types/network"
	"github.com/moby/moby/client"
	"github.com/stretchr/testify/require"
)

func TestWorkflowResourceLabels(t *testing.T) {
	labels := workflowResourceLabels("agent-a", "task-a", dockerResourceNetwork)

	require.Equal(t, "true", labels[dockerLabelManaged])
	require.Equal(t, dockerResourceNetwork, labels[dockerLabelResource])
	require.Equal(t, "agent-a", labels[dockerLabelAgent])
	require.Equal(t, "task-a", labels[dockerLabelTask])
	require.True(t, hasWorkflowResourceLabels(labels, "agent-a", "task-a", dockerResourceNetwork))
	require.False(t, hasWorkflowResourceLabels(labels, "agent-b", "task-a", dockerResourceNetwork))
	require.False(t, hasWorkflowResourceLabels(labels, "agent-a", "task-b", dockerResourceNetwork))
}

func TestNewNetworkBackOffIsExponential(t *testing.T) {
	retryBackOff := newNetworkBackOff()
	retryBackOff.Reset()

	first := retryBackOff.NextBackOff()
	second := retryBackOff.NextBackOff()
	third := retryBackOff.NextBackOff()

	require.Greater(t, second, first)
	require.Greater(t, third, second)
}

func TestRemoveNetworkWithBackOffRetriesOnlyTransientErrors(t *testing.T) {
	t.Run("bounded retries", func(t *testing.T) {
		attempts := 0
		err := removeNetworkWithBackOff(t.Context(), func() error {
			attempts++
			return fmt.Errorf("network has active endpoints: %w", errdefs.ErrPermissionDenied)
		}, &backoff.ZeroBackOff{})

		require.Error(t, err)
		require.Equal(t, int(maxRetry), attempts)
	})

	t.Run("eventual success", func(t *testing.T) {
		attempts := 0
		err := removeNetworkWithBackOff(t.Context(), func() error {
			attempts++
			if attempts < int(maxRetry) {
				return fmt.Errorf("network has active endpoints: %w", errdefs.ErrPermissionDenied)
			}
			return nil
		}, &backoff.ZeroBackOff{})

		require.NoError(t, err)
		require.Equal(t, int(maxRetry), attempts)
	})

	t.Run("generic permission denied is not retried", func(t *testing.T) {
		attempts := 0
		err := removeNetworkWithBackOff(t.Context(), func() error {
			attempts++
			return fmt.Errorf("authorization denied: %w", errdefs.ErrPermissionDenied)
		}, &backoff.ZeroBackOff{})

		require.Error(t, err)
		require.Equal(t, 1, attempts)
	})

	t.Run("non retryable", func(t *testing.T) {
		attempts := 0
		sentinel := errors.New("permission denied")
		err := removeNetworkWithBackOff(t.Context(), func() error {
			attempts++
			return sentinel
		}, &backoff.ZeroBackOff{})

		require.ErrorIs(t, err, sentinel)
		require.Equal(t, 1, attempts)
	})

	t.Run("already gone", func(t *testing.T) {
		attempts := 0
		err := removeNetworkWithBackOff(t.Context(), func() error {
			attempts++
			return fmt.Errorf("gone: %w", errdefs.ErrNotFound)
		}, &backoff.ZeroBackOff{})

		require.NoError(t, err)
		require.Equal(t, 1, attempts)
	})
}

func TestCleanupOrphanNetworksIsOwnerScopedAndRequiresEmptyNetwork(t *testing.T) {
	ownedEmpty := network.Summary{Network: network.Network{
		ID:     "owned-empty-id",
		Name:   "owned-empty",
		Labels: workflowResourceLabels("agent-a", "task-1", dockerResourceNetwork),
	}}
	ownedActive := network.Summary{Network: network.Network{
		ID:     "owned-active-id",
		Name:   "owned-active",
		Labels: workflowResourceLabels("agent-a", "task-2", dockerResourceNetwork),
	}}
	otherOwner := network.Summary{Network: network.Network{
		ID:     "other-owner-id",
		Name:   "other-owner",
		Labels: workflowResourceLabels("agent-b", "task-3", dockerResourceNetwork),
	}}
	unmanaged := network.Summary{Network: network.Network{
		ID:   "unmanaged-id",
		Name: "wp_legacy_without_labels",
	}}

	fake := &fakeNetworkCleanupClient{
		list: client.NetworkListResult{Items: []network.Summary{ownedEmpty, ownedActive, otherOwner, unmanaged}},
		inspect: map[string]client.NetworkInspectResult{
			"owned-empty-id": {
				Network: network.Inspect{Network: ownedEmpty.Network},
			},
			"owned-active-id": {
				Network: network.Inspect{
					Network: ownedActive.Network,
					Containers: map[string]network.EndpointResource{
						"container-id": {},
					},
				},
			},
		},
	}

	err := cleanupOrphanNetworks(t.Context(), fake, "agent-a")

	require.NoError(t, err)
	require.Equal(t, []string{"owned-empty-id"}, fake.removed)
	require.Equal(t, []string{"owned-empty-id", "owned-active-id"}, fake.inspected)
}

type fakeNetworkCleanupClient struct {
	list       client.NetworkListResult
	listErr    error
	inspect    map[string]client.NetworkInspectResult
	inspectErr map[string]error
	removeErr  map[string]error
	inspected  []string
	removed    []string
}

func (f *fakeNetworkCleanupClient) NetworkList(context.Context, client.NetworkListOptions) (client.NetworkListResult, error) {
	return f.list, f.listErr
}

func (f *fakeNetworkCleanupClient) NetworkInspect(_ context.Context, networkID string, _ client.NetworkInspectOptions) (client.NetworkInspectResult, error) {
	f.inspected = append(f.inspected, networkID)
	if err := f.inspectErr[networkID]; err != nil {
		return client.NetworkInspectResult{}, err
	}
	return f.inspect[networkID], nil
}

func (f *fakeNetworkCleanupClient) NetworkRemove(_ context.Context, networkID string, _ client.NetworkRemoveOptions) (client.NetworkRemoveResult, error) {
	f.removed = append(f.removed, networkID)
	if err := f.removeErr[networkID]; err != nil {
		return client.NetworkRemoveResult{}, err
	}
	return client.NetworkRemoveResult{}, nil
}
