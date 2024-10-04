// SPDX-License-Identifier: BUSL-1.1
//
// Copyright (C) 2024, Berachain Foundation. All rights reserved.
// Use of this software is governed by the Business Source License included
// in the LICENSE file of this repository and at www.mariadb.com/bsl11.
//
// ANY USE OF THE LICENSED WORK IN VIOLATION OF THIS LICENSE WILL AUTOMATICALLY
// TERMINATE YOUR RIGHTS UNDER THIS LICENSE FOR THE CURRENT AND ALL OTHER
// VERSIONS OF THE LICENSED WORK.
//
// THIS LICENSE DOES NOT GRANT YOU ANY RIGHT IN ANY TRADEMARK OR LOGO OF
// LICENSOR OR ITS AFFILIATES (PROVIDED THAT YOU MAY USE A TRADEMARK OR LOGO OF
// LICENSOR AS EXPRESSLY REQUIRED BY THIS LICENSE).
//
// TO THE EXTENT PERMITTED BY APPLICABLE LAW, THE LICENSED WORK IS PROVIDED ON
// AN “AS IS” BASIS. LICENSOR HEREBY DISCLAIMS ALL WARRANTIES AND CONDITIONS,
// EXPRESS OR IMPLIED, INCLUDING (WITHOUT LIMITATION) WARRANTIES OF
// MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE, NON-INFRINGEMENT, AND
// TITLE.

package blockchain

import (
	"context"
	"sync"

	asynctypes "github.com/berachain/beacon-kit/mod/async/pkg/types"
	"github.com/berachain/beacon-kit/mod/log"
	"github.com/berachain/beacon-kit/mod/primitives/pkg/async"
	"github.com/berachain/beacon-kit/mod/primitives/pkg/common"
	"github.com/berachain/beacon-kit/mod/primitives/pkg/transition"
)

// Service represents the blockchain service.
type Service[
	AvailabilityStoreT AvailabilityStore[BeaconBlockBodyT],
	BeaconBlockT BeaconBlock[BeaconBlockBodyT],
	BeaconBlockBodyT BeaconBlockBody[ExecutionPayloadT],
	BeaconBlockHeaderT BeaconBlockHeader,
	BeaconStateT ReadOnlyBeaconState[
		BeaconStateT, BeaconBlockHeaderT, ExecutionPayloadHeaderT,
	],
	DepositT any,
	ExecutionPayloadT ExecutionPayload,
	ExecutionPayloadHeaderT ExecutionPayloadHeader,
	GenesisT Genesis[DepositT, ExecutionPayloadHeaderT],
	PayloadAttributesT PayloadAttributes,
] struct {
	storageBackend           StorageBackend[AvailabilityStoreT, BeaconStateT]
	logger                   log.Logger
	chainSpec                common.ChainSpec
	dispatcher               asynctypes.Dispatcher
	executionEngine          ExecutionEngine[PayloadAttributesT]
	localBuilder             LocalBuilder[BeaconStateT]
	stateProcessor           StateProcessor[
		BeaconBlockT,
		BeaconStateT,
		*transition.Context,
		DepositT,
		ExecutionPayloadHeaderT,
	]
	metrics                  *chainMetrics
	optimisticPayloadBuilds   bool
	forceStartupSyncOnce      *sync.Once
	subFinalBlkReceived       chan async.Event[BeaconBlockT]
	subBlockReceived          chan async.Event[BeaconBlockT]
	subGenDataReceived        chan async.Event[GenesisT]
}

// NewService initializes and returns a new Service instance.
func NewService[
	AvailabilityStoreT AvailabilityStore[BeaconBlockBodyT],
	BeaconBlockT BeaconBlock[BeaconBlockBodyT],
	BeaconBlockBodyT BeaconBlockBody[ExecutionPayloadT],
	BeaconBlockHeaderT BeaconBlockHeader,
	BeaconStateT ReadOnlyBeaconState[
		BeaconStateT, BeaconBlockHeaderT, ExecutionPayloadHeaderT,
	],
	DepositT any,
	ExecutionPayloadT ExecutionPayload,
	ExecutionPayloadHeaderT ExecutionPayloadHeader,
	GenesisT Genesis[DepositT, ExecutionPayloadHeaderT],
	PayloadAttributesT PayloadAttributes,
](
	storageBackend StorageBackend[AvailabilityStoreT, BeaconStateT],
	logger log.Logger,
	chainSpec common.ChainSpec,
	dispatcher asynctypes.Dispatcher,
	executionEngine ExecutionEngine[PayloadAttributesT],
	localBuilder LocalBuilder[BeaconStateT],
	stateProcessor StateProcessor[
		BeaconBlockT,
		BeaconStateT,
		*transition.Context,
		DepositT,
		ExecutionPayloadHeaderT,
	],
	telemetrySink TelemetrySink,
	optimisticPayloadBuilds bool,
) *Service[
	AvailabilityStoreT, BeaconBlockT, BeaconBlockBodyT, BeaconBlockHeaderT,
	BeaconStateT, DepositT, ExecutionPayloadT, ExecutionPayloadHeaderT,
	GenesisT, PayloadAttributesT,
] {
	return &Service[
		AvailabilityStoreT, BeaconBlockT, BeaconBlockBodyT, BeaconBlockHeaderT,
		BeaconStateT, DepositT, ExecutionPayloadT, ExecutionPayloadHeaderT,
		GenesisT, PayloadAttributesT,
	]{
		storageBackend:          storageBackend,
		logger:                  logger,
		chainSpec:               chainSpec,
		dispatcher:              dispatcher,
		executionEngine:         executionEngine,
		localBuilder:            localBuilder,
		stateProcessor:          stateProcessor,
		metrics:                 newChainMetrics(telemetrySink),
		optimisticPayloadBuilds: optimisticPayloadBuilds,
		forceStartupSyncOnce:    new(sync.Once),
		subFinalBlkReceived:     make(chan async.Event[BeaconBlockT]),
		subBlockReceived:        make(chan async.Event[BeaconBlockT]),
		subGenDataReceived:      make(chan async.Event[GenesisT]),
	}
}

// Name returns the name of the service.
func (s *Service[
	_, _, _, _, _, _, _, _, _, _,
]) Name() string {
	return "blockchain"
}

// Start subscribes the Blockchain service to various events and starts the main event loop.
func (s *Service[
	_, _, _, _, _, _, _, _, _, _,
]) Start(ctx context.Context) error {
	if err := s.subscribeToEvents(); err != nil {
		return err
	}
	go s.eventLoop(ctx)
	return nil
}

// subscribeToEvents subscribes to all necessary events for the blockchain service.
func (s *Service[
	_, _, _, _, _, _, _, _, _, _,
]) subscribeToEvents() error {
	subscriptions := []struct {
		eventType async.EventType
		channel   interface{}
	}{
		{async.GenesisDataReceived, s.subGenDataReceived},
		{async.BeaconBlockReceived, s.subBlockReceived},
		{async.FinalBeaconBlockReceived, s.subFinalBlkReceived},
	}
	for _, sub := range subscriptions {
		if err := s.dispatcher.Subscribe(sub.eventType, sub.channel); err != nil {
			return err
		}
	}
	return nil
}

// eventLoop listens for events and handles them accordingly.
func (s *Service[
	_, BeaconBlockT, _, _, _, _, _, _, GenesisT, _,
]) eventLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case event := <-s.subGenDataReceived:
			s.handleEvent(event, s.handleGenDataReceived)
		case event := <-s.subBlockReceived:
			s.handleEvent(event, s.handleBeaconBlockReceived)
		case event := <-s.subFinalBlkReceived:
			s.handleEvent(event, s.handleBeaconBlockFinalization)
		}
	}
}

// handleEvent abstracts common event handling logic to avoid redundancy.
func (s *Service[
	_, BeaconBlockT, _, _, _, _, _, _, _, _,
]) handleEvent[T any](msg async.Event[T], handler func(async.Event[T])) {
	if msg.Error() != nil {
		s.logger.Error("Error processing event", "error", msg.Error())
		return
	}
	handler(msg)
}

// handleGenDataReceived processes genesis data and emits an event with the result.
func (s *Service[
	_, _, _, _, _, _, _, _, GenesisT, _,
]) handleGenDataReceived(msg async.Event[GenesisT]) {
	valUpdates, err := s.ProcessGenesisData(msg.Context(), msg.Data())
	if err != nil {
		s.logger.Error("Failed to process genesis data", "error", err)
	}
	s.publishEvent(msg, async.GenesisDataProcessed, valUpdates, err)
}

// handleBeaconBlockReceived processes a received block and emits a verification event.
func (s *Service[
	_, BeaconBlockT, _, _, _, _, _, _, _, _,
]) handleBeaconBlockReceived(msg async.Event[BeaconBlockT]) {
	err := s.VerifyIncomingBlock(msg.Context(), msg.Data())
	s.publishEvent(msg, async.BeaconBlockVerified, msg.Data(), err)
}

// handleBeaconBlockFinalization processes a finalized block and emits a validator updates event.
func (s *Service[
	_, BeaconBlockT, _, _, _, _, _, _, _, _,
]) handleBeaconBlockFinalization(msg async.Event[BeaconBlockT]) {
	valUpdates, err := s.ProcessBeaconBlock(msg.Context(), msg.Data())
	s.publishEvent(msg, async.FinalValidatorUpdatesProcessed, valUpdates, err)
}

// publishEvent abstracts the logic for emitting an event to reduce duplication.
func (s *Service[
	_, BeaconBlockT, _, _, _, _, _, _, _, _,
]) publishEvent[T any](msg async.Event[T], eventType async.EventType, data any, err error) {
	if err := s.dispatcher.Publish(
		async.NewEvent(msg.Context(), eventType, data, err),
	); err != nil {
		s.logger.Error("Failed to publish event", "error", err)
		panic(err)
	}
}

