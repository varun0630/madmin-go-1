//
// Copyright (c) 2015-2026 MinIO, Inc.
//
// This file is part of MinIO Object Storage stack
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.
//

package madmin

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"time"
)

// RepatriateStartOptions contains parameters for starting a repatriation operation.
type RepatriateStartOptions struct {
	// Bucket is the bucket to repatriate. Required.
	Bucket string `json:"bucket"`
}

// RepatriateStopOptions contains parameters for stopping a repatriation.
type RepatriateStopOptions struct {
	// Bucket is the bucket whose repatriation session should be stopped. Required.
	Bucket string `json:"bucket"`
}

// RepatriateStatus is the status of a repatriation session.
type RepatriateStatus string

const (
	RepatriateStatusStarted   RepatriateStatus = "started"
	RepatriateStatusCompleted RepatriateStatus = "completed"
	RepatriateStatusFailed    RepatriateStatus = "failed"
	RepatriateStatusStopped   RepatriateStatus = "stopped"
)

// RepatBucketStatus contains the status and progress of a single bucket's repatriation session.
type RepatBucketStatus struct {
	ID              string           `json:"id"`
	Bucket          string           `json:"bucket"`
	Status          RepatriateStatus `json:"status"`
	StartTime       time.Time        `json:"startTime"`
	EndTime         time.Time        `json:"endTime,omitempty"`
	StoppedAt       time.Time        `json:"stoppedAt,omitempty"`
	Object          string           `json:"object"`
	LastKey         string           `json:"lastKey,omitempty"`
	NumObjectsTotal uint64           `json:"numObjectsTotal"`
	NumObjects      uint64           `json:"numObjects"`
	NumVersions     uint64           `json:"numVersions"`
	BytesDone       uint64           `json:"bytesDone"`
	BytesTotal      uint64           `json:"bytesTotal"`
	// ETASeconds is nil when ETA cannot be computed (session paused, no
	// bytes done yet, or BytesTotal unknown).
	ETASeconds *int64 `json:"etaSeconds,omitempty"`
}

// RepatriateStatusInfo contains the current status of all bucket repatriation sessions.
type RepatriateStatusInfo struct {
	Buckets []RepatBucketStatus `json:"buckets"`
}

// RepatriateStart starts a repatriation operation for the specified bucket.
func (adm *AdminClient) RepatriateStart(ctx context.Context, opts RepatriateStartOptions) error {
	body, err := json.Marshal(opts)
	if err != nil {
		return err
	}

	resp, err := adm.executeMethod(ctx,
		http.MethodPost,
		requestData{
			relPath: adminAPIPrefix + "/repatriate/start",
			content: body,
		})
	defer closeResponse(resp)
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusOK {
		return httpRespToErrorResponse(resp)
	}
	return nil
}

// RepatriateStatus returns the current status of repatriation sessions.
func (adm *AdminClient) RepatriateStatus(ctx context.Context) (RepatriateStatusInfo, error) {
	var info RepatriateStatusInfo

	resp, err := adm.executeMethod(ctx,
		http.MethodGet,
		requestData{relPath: adminAPIPrefix + "/repatriate/status"})
	defer closeResponse(resp)
	if err != nil {
		return info, err
	}

	if resp.StatusCode != http.StatusOK {
		return info, httpRespToErrorResponse(resp)
	}

	respBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return info, err
	}
	if err = json.Unmarshal(respBytes, &info); err != nil {
		return info, err
	}
	return info, nil
}

// RepatriateStop stops a running repatriation for the specified bucket. The
// session transitions to "stopped" and any in-flight cross-pool moves are
// rolled back so the source pool remains the consistent home for affected
// objects.
func (adm *AdminClient) RepatriateStop(ctx context.Context, opts RepatriateStopOptions) error {
	body, err := json.Marshal(opts)
	if err != nil {
		return err
	}

	resp, err := adm.executeMethod(ctx,
		http.MethodPost,
		requestData{
			relPath: adminAPIPrefix + "/repatriate/stop",
			content: body,
		})
	defer closeResponse(resp)
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusOK {
		return httpRespToErrorResponse(resp)
	}
	return nil
}

