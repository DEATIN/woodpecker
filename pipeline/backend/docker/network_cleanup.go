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
	"strings"
	"time"

	"github.com/cenkalti/backoff/v7"
	"github.com/containerd/errdefs"
	"github.com/moby/moby/client"
)

const (
	dockerLabelManaged  = "wp_managed"
	dockerLabelResource = "wp_resource"
	dockerLabelAgent    = "wp_agent"
	dockerLabelTask     = "wp_task"

	dockerResourceNetwork = "network"
	dockerResourceVolume  = "volume"
	dockerResourceStep    = "step"

	networkRetryWait             = 500 * time.Millisecond
	networkMaxRetryWait          = 2 * time.Second
	workflowCleanupTimeout       = 30 * time.Second
	workflowSetupRollbackTimeout = 15 * time.Second
)

type networkCleanupClient interface {
	NetworkList(context.Context, client.NetworkListOptions) (client.NetworkListResult, error)
	NetworkInspect(context.Context, string, client.NetworkInspectOptions) (client.NetworkInspectResult, error)
	NetworkRemove(context.Context, string, client.NetworkRemoveOptions) (client.NetworkRemoveResult, error)
}

func workflowResourceLabels(owner, taskUUID, resource string) map[string]string {
	labels := map[string]string{
		dockerLabelManaged:  "true",
		dockerLabelResource: resource,
		dockerLabelTask:     taskUUID,
	}
	if owner != "" {
		labels[dockerLabelAgent] = owner
	}
	return labels
}

func hasWorkflowResourceLabels(labels map[string]string, owner, taskUUID, resource string) bool {
	if owner == "" || labels[dockerLabelManaged] != "true" || labels[dockerLabelResource] != resource || labels[dockerLabelAgent] != owner {
		return false
	}
	return taskUUID == "" || labels[dockerLabelTask] == taskUUID
}

func isRetryableNetworkRemoveError(err error) bool {
	// Docker reports "network has active endpoints" as HTTP 403/Forbidden,
	// which the Moby client maps to containerd's PermissionDenied sentinel.
	activeEndpoints := errdefs.IsPermissionDenied(err) && strings.Contains(err.Error(), "has active endpoints")
	return errdefs.IsConflict(err) || errdefs.IsFailedPrecondition(err) || errdefs.IsUnavailable(err) || activeEndpoints
}

func removeNetworkWithRetry(ctx context.Context, remove func() error) error {
	return removeNetworkWithBackOff(ctx, remove, newNetworkBackOff())
}

func removeNetworkWithBackOff(ctx context.Context, remove func() error, retryBackOff backoff.BackOff) error {
	_, err := backoff.Retry(ctx, func() (struct{}, error) {
		err := remove()
		switch {
		case err == nil, errdefs.IsNotFound(err):
			return struct{}{}, nil
		case isRetryableNetworkRemoveError(err):
			return struct{}{}, err
		default:
			return struct{}{}, backoff.Permanent(err)
		}
	}, backoff.WithMaxTries(maxRetry), backoff.WithBackOff(retryBackOff))
	return err
}

func newNetworkBackOff() backoff.BackOff {
	return &backoff.ExponentialBackOff{
		InitialInterval:     networkRetryWait,
		RandomizationFactor: 0,
		Multiplier:          2, //nolint:mnd
		MaxInterval:         networkMaxRetryWait,
	}
}

// removeOwnedEmptyNetworkWithRetry removes a network only while it is still
// positively identified as an empty Woodpecker workflow network owned by this
// agent. Ownership and emptiness are re-checked before every removal attempt to
// avoid deleting a network that became active between list/inspect/remove calls.
func removeOwnedEmptyNetworkWithRetry(ctx context.Context, c networkCleanupClient, networkID, owner, taskUUID string) error {
	_, err := backoff.Retry(ctx, func() (struct{}, error) {
		inspect, err := c.NetworkInspect(ctx, networkID, client.NetworkInspectOptions{})
		if err != nil {
			switch {
			case errdefs.IsNotFound(err):
				return struct{}{}, nil
			case isRetryableNetworkRemoveError(err):
				return struct{}{}, err
			default:
				return struct{}{}, backoff.Permanent(err)
			}
		}

		n := inspect.Network
		if !hasWorkflowResourceLabels(n.Labels, owner, taskUUID, dockerResourceNetwork) {
			return struct{}{}, nil
		}
		if len(n.Containers) != 0 || len(n.Services) != 0 {
			return struct{}{}, nil
		}

		identifier := n.ID
		if identifier == "" {
			identifier = networkID
		}
		_, err = c.NetworkRemove(ctx, identifier, client.NetworkRemoveOptions{})
		switch {
		case err == nil, errdefs.IsNotFound(err):
			return struct{}{}, nil
		case isRetryableNetworkRemoveError(err):
			return struct{}{}, err
		default:
			return struct{}{}, backoff.Permanent(err)
		}
	}, backoff.WithMaxTries(maxRetry), backoff.WithBackOff(newNetworkBackOff()))
	return err
}

// cleanupOrphanNetworks removes only empty, explicitly Woodpecker-managed
// workflow networks owned by the current agent. It deliberately does not use
// name-prefix matching or Docker's broad network prune operation.
func cleanupOrphanNetworks(ctx context.Context, c networkCleanupClient, owner string) error {
	if owner == "" {
		return nil
	}

	list, err := c.NetworkList(ctx, client.NetworkListOptions{})
	if err != nil {
		return fmt.Errorf("list docker networks for orphan cleanup: %w", err)
	}

	var cleanupErr error
	for _, candidate := range list.Items {
		if !hasWorkflowResourceLabels(candidate.Labels, owner, "", dockerResourceNetwork) {
			continue
		}
		identifier := candidate.ID
		if identifier == "" {
			identifier = candidate.Name
		}
		if identifier == "" {
			continue
		}
		if err := removeOwnedEmptyNetworkWithRetry(ctx, c, identifier, owner, ""); err != nil {
			cleanupErr = errors.Join(cleanupErr, fmt.Errorf("remove orphan docker network %q: %w", candidate.Name, err))
		}
	}
	return cleanupErr
}
