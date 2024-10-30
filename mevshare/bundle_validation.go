package mevshare

import (
	"errors"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
	"golang.org/x/crypto/sha3"
)

var (
	ErrUnsupportedBundleVersion = errors.New("unsupported bundle version")
	ErrBundleTooDeep            = errors.New("bundle too deep")
	ErrInvalidBundleConstraints = errors.New("invalid bundle constraints")
	ErrInvalidRevertMode        = errors.New("invalid revert mode")
	ErrInvalidBundlePrivacy     = errors.New("invalid bundle privacy")
)

// MergeMaxBlock writes to the topLevel inclusion value of overlap between inner and topLevel
// or return error if there is no overlap
func MergeMaxBlock(topLevel hexutil.Uint64, inner hexutil.Uint64) hexutil.Uint64 {
	if topLevel > inner {
		topLevel = inner
	}
	return topLevel
}

func validateBundleInner(level int, bundle *SendMevBundleArgs, currentBlock uint64, signer types.Signer) (hash common.Hash, txs int, unmatched bool, err error) { //nolint:gocognit,gocyclo
	if level > MaxNestingLevel {
		return hash, txs, unmatched, ErrBundleTooDeep
	}
	// validate inclusion
	if bundle.MaxBlock == 0 {
		bundle.MaxBlock = hexutil.Uint64(currentBlock + 30)
	}
	//允许都为0的请求发过来，以支持rpc-endpoint
	maxBlock := uint64(bundle.MaxBlock)
	if currentBlock >= maxBlock {
		return hash, txs, unmatched, ErrInvalidInclusion
	}
	if len(bundle.Txs) == 0 {
		return hash, txs, unmatched, ErrInvalidBundleBodySize
	}
	var bodyHashes []common.Hash
	if bundle.Hash != nil {
		unmatched = true
		bodyHashes = append(bodyHashes, *bundle.Hash)
		txs++
	} else if bundle.Txs != nil {
		for _, data := range bundle.Txs {
			var tx types.Transaction
			err := tx.UnmarshalBinary(data)
			if err != nil {
				return hash, txs, unmatched, err
			}
			bodyHashes = append(bodyHashes, tx.Hash())
			txs++
		}
	} else if bundle.Bundle != nil {
		bundle.MaxBlock = MergeMaxBlock(bundle.MaxBlock, bundle.Bundle.MaxBlock)
		// hash, tx count, has backrun?
		h, t, u, err := validateBundleInner(level+1, bundle.Bundle, currentBlock, signer)
		if err != nil {
			return hash, txs, unmatched, err
		}
		bodyHashes = append(bodyHashes, h)
		txs += t
		// don't allow unmatched bundles below 1-st level
		if u {
			return hash, txs, unmatched, ErrInvalidBundleBody
		}
	}

	if txs > MaxBodySize {
		return hash, txs, unmatched, ErrInvalidBundleBodySize
	}

	if len(bodyHashes) == 1 {
		// special case of bundle with a single tx
		hash = bodyHashes[0]
	} else {
		hasher := sha3.NewLegacyKeccak256()
		for _, h := range bodyHashes {
			hasher.Write(h[:])
		}
		hash = common.BytesToHash(hasher.Sum(nil))
	}

	if bundle.Hints != HintNone {
		bundle.Hints.SetHint(HintHash)
	}
	if bundle.RefundPercent < 0 || bundle.RefundPercent > 100 {
		return hash, txs, unmatched, ErrInvalidBundlePrivacy
	}

	// clean fields owned by the node
	bundle.Metadata = &MevBundleMetadata{}
	bundle.Metadata.BundleHash = hash
	bundle.Metadata.BodyHashes = bodyHashes
	bundle.Metadata.ReplacementNonce = 0
	matchingHasher := sha3.NewLegacyKeccak256()
	matchingHasher.Write(hash[:])
	matchingHash := common.BytesToHash(matchingHasher.Sum(nil))
	bundle.Metadata.MatchingHash = matchingHash

	return hash, txs, unmatched, nil
}

func ValidateBundle(bundle *SendMevBundleArgs, currentBlock uint64, signer types.Signer) (hash common.Hash, unmatched bool, err error) {
	hash, _, unmatched, err = validateBundleInner(0, bundle, currentBlock, signer)
	return hash, unmatched, err
}
