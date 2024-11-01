package mevshare

import (
	"context"
	"errors"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"math/rand"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/lru"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/flashbots/mev-share-node/adapters/redis"
	"github.com/flashbots/mev-share-node/jsonrpcserver"
	"github.com/flashbots/mev-share-node/metrics"
	"github.com/flashbots/mev-share-node/spike"
	"go.uber.org/zap"
	"golang.org/x/time/rate"
)

var (
	ErrInvalidInclusion      = errors.New("invalid inclusion")
	ErrInvalidBundleBodySize = errors.New("invalid bundle body size")
	ErrInvalidBundleBody     = errors.New("invalid bundle body")
	ErrBackrunNotFound       = errors.New("backrun not found")
	ErrBackrunInvalidBundle  = errors.New("backrun invalid bundle")
	ErrBackrunInclusion      = errors.New("backrun invalid inclusion")

	ErrInternalServiceError = errors.New("mev-share service error")

	simBundleTimeout    = 500 * time.Millisecond
	cancelBundleTimeout = 3 * time.Second
	bundleCacheSize     = 1000
)

type SimScheduler interface {
	ScheduleBundleSimulation(ctx context.Context, bundle *SendMevBundleArgs, highPriority bool) error
}

type BundleStorage interface {
	GetBundleByMatchingHash(ctx context.Context, hash common.Hash) (*SendMevBundleArgs, error)
	CancelBundleByHash(ctx context.Context, hash common.Hash, signer common.Address) error
}

type EthClient interface {
	BlockNumber(ctx context.Context) (uint64, error)
}

type API struct {
	log *zap.Logger

	scheduler      SimScheduler
	bundleStorage  BundleStorage
	eth            EthClient
	signer         types.Signer
	simBackends    []SimulationBackend
	simRateLimiter *rate.Limiter
	builders       BuildersBackend

	spikeManager      *spike.Manager[*SendMevBundleArgs]
	knownBundleCache  *lru.Cache[common.Hash, SendMevBundleArgs]
	cancellationCache *RedisCancellationCache
	//replacementCache  *redis.ReplacementCache
}

func NewAPI(
	log *zap.Logger,
	scheduler SimScheduler, bundleStorage BundleStorage, eth EthClient, signer types.Signer,
	simBackends []SimulationBackend, simRateLimit rate.Limit, builders BuildersBackend, cancellationCache *RedisCancellationCache,
	replacementCache *redis.ReplacementCache,
	sbundleValidDuration time.Duration,
) *API {
	sm := spike.NewManager(func(ctx context.Context, k string) (*SendMevBundleArgs, error) {
		return bundleStorage.GetBundleByMatchingHash(ctx, common.HexToHash(k))
	}, sbundleValidDuration)

	return &API{
		log: log,

		scheduler:         scheduler,
		bundleStorage:     bundleStorage,
		eth:               eth,
		signer:            signer,
		simBackends:       simBackends,
		simRateLimiter:    rate.NewLimiter(simRateLimit, 1),
		builders:          builders,
		spikeManager:      sm,
		knownBundleCache:  lru.NewCache[common.Hash, SendMevBundleArgs](bundleCacheSize),
		cancellationCache: cancellationCache,
		//replacementCache:  replacementCache,
	}
}

func (m *API) SendBundle(ctx context.Context, bundle SendMevBundleArgs) (_ SendMevBundleResponse, err error) {
	logger := m.log
	startAt := time.Now()
	defer func() {
		metrics.RecordRPCCallDuration(SendBundleEndpointName, time.Since(startAt).Milliseconds())
	}()
	metrics.IncSbundlesReceived()

	validateBundleTime := time.Now()
	currentBlock, err := m.eth.BlockNumber(ctx)
	if err != nil {
		metrics.IncRPCCallFailure(SendBundleEndpointName)
		logger.Error("failed to get current block", zap.Error(err))
		return SendMevBundleResponse{}, ErrInternalServiceError
	}

	hash, hasUnmatchedHash, err := ValidateBundle(&bundle, currentBlock, m.signer)
	if err != nil {
		logger.Warn("failed to validate bundle", zap.Error(err))
		return SendMevBundleResponse{}, err
	}
	logger.Debug("received bundle", zap.String("bundle", hash.String()), zap.Time("receivedAt", startAt), zap.Int64("timestamp", startAt.Unix()))

	if oldBundle, ok := m.knownBundleCache.Get(hash); ok {
		if !newerInclusion(&oldBundle, &bundle) {
			logger.Debug("bundle already known, ignoring", zap.String("hash", hash.Hex()))
			return SendMevBundleResponse{hash}, nil
		}
	}
	m.knownBundleCache.Add(hash, bundle)
	if bundle.Metadata == nil {
		bundle.Metadata = &MevBundleMetadata{}
	}
	bundle.Metadata.ReceivedAt = hexutil.Uint64(uint64(time.Now().UnixMicro()))
	bundle.Metadata.Prematched = !hasUnmatchedHash

	metrics.RecordBundleValidationDuration(time.Since(validateBundleTime).Milliseconds())

	if hasUnmatchedHash {
		var unmatchedHash common.Hash = *bundle.Hash
		fetchUnmatchedTime := time.Now()
		unmatchedBundle, err := m.spikeManager.GetResult(ctx, unmatchedHash.String())
		metrics.RecordBundleFetchUnmatchedDuration(time.Since(fetchUnmatchedTime).Milliseconds())
		if err != nil {
			logger.Error("Failed to fetch unmatched bundle", zap.Error(err), zap.String("matching_hash", unmatchedHash.Hex()))
			metrics.IncRPCCallFailure(SendBundleEndpointName)
			return SendMevBundleResponse{}, ErrBackrunNotFound
		}
		if !unmatchedBundle.Hints.HasHint(HintHash) {
			// if the unmatched bundle have not configured privacy or has not set the hash hint
			// then we cannot backrun it
			logger.Error("unmatched bundle has no hash hint", zap.String("hash", unmatchedHash.Hex()))
			return SendMevBundleResponse{}, ErrBackrunInvalidBundle
		}
		bundle.Bundle = unmatchedBundle
		bundle.Hash = nil
		bundle.MaxBlock = MergeMaxBlock(bundle.MaxBlock, unmatchedBundle.MaxBlock)
	}

	metrics.IncSbundlesReceivedValid()
	highPriority := jsonrpcserver.GetPriority(ctx)
	err = m.scheduler.ScheduleBundleSimulation(ctx, &bundle, highPriority)
	if err != nil {
		metrics.IncRPCCallFailure(SendBundleEndpointName)
		logger.Error("Failed to schedule bundle simulation", zap.Error(err))
		return SendMevBundleResponse{}, ErrInternalServiceError
	}

	return SendMevBundleResponse{
		BundleHash: hash,
	}, nil
}

func (m *API) SimBundle(ctx context.Context, bundle SendMevBundleArgs, aux SimMevBundleAuxArgs) (_ *SimMevBundleResponse, err error) {
	startAt := time.Now()
	defer func() {
		metrics.RecordRPCCallDuration(SimBundleEndpointName, time.Since(startAt).Milliseconds())
		if err != nil {
			metrics.IncRPCCallFailure(SimBundleEndpointName)
		}
	}()

	if len(m.simBackends) == 0 {
		return nil, ErrInternalServiceError
	}
	ctx, cancel := context.WithTimeout(ctx, simBundleTimeout)
	defer cancel()

	simTimeout := int64(simBundleTimeout / time.Millisecond)
	aux.Timeout = &simTimeout

	err = m.simRateLimiter.Wait(ctx)
	if err != nil {
		return nil, err
	}

	// select random backend
	idx := rand.Intn(len(m.simBackends)) //nolint:gosec
	backend := m.simBackends[idx]
	return backend.SimulateBundle(ctx, &bundle, &aux)
}

// CancelBundleByHash cancels a bundle by hash
// This method is not exposed on the bundle relay.
// However, it is used by the Flashbots bundle relay for now to handle the cancellation of private transactions.
func (m *API) CancelBundleByHash(ctx context.Context, hash common.Hash) (err error) {
	startAt := time.Now()
	defer func() {
		metrics.RecordRPCCallDuration(CancelBundleByHashEndpointName, time.Since(startAt).Milliseconds())
		if err != nil {
			metrics.IncRPCCallFailure(CancelBundleByHashEndpointName)
		}
	}()
	logger := m.log.With(zap.String("bundle", hash.Hex()))
	ctx, cancel := context.WithTimeout(ctx, cancelBundleTimeout)
	defer cancel()
	signerAddress := jsonrpcserver.GetSigner(ctx)
	err = m.bundleStorage.CancelBundleByHash(ctx, hash, signerAddress)
	if err != nil {
		if !errors.Is(err, ErrBundleNotCancelled) {
			logger.Warn("Failed to cancel bundle", zap.Error(err))
		}
		return ErrBundleNotCancelled
	}

	err = m.cancellationCache.Add(ctx, hash)
	if err != nil {
		logger.Error("Failed to add bundle to cancellation cache", zap.Error(err))
	}

	logger.Info("Bundle cancelled")
	return nil
}
