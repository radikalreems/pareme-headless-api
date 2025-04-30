package inout

import (
	"bytes"
	"fmt"
	"math/big"
	"os"
	"pareme/common"
	"sort"
	"time"
)

const (
	adjustmentInterval int     = 2016                                 // Number of blocks in difficulty adjustment period
	targetBlockTime    int     = 10 * 1000                            // Target block time in milliseconds
	targetPeriodTime   int     = adjustmentInterval * targetBlockTime // Total target time for adjustment period
	minDifficultyRatio float64 = 0.25                                 // Minimum allowed difficulty adjustment ratio
	maxDifficultyRatio float64 = 4                                    // Maximum allowed difficulty adjustment ratio
)

// Given a potential block, calculate the expected difficulty target.
func CalculateDifficulty(nextBlock common.Block) ([32]byte, [4]byte, error) {
	common.PrintToLog("Calculating Difficulty...")

	// Get adjustment ratio for difficulty
	ratio, err := calculateRatio(nextBlock)
	if err != nil {
		return [32]byte{}, [4]byte{}, fmt.Errorf("ratio calculation failed: %v", err)
	}

	// Convert previous nBits to target
	prevTarget := common.NBitsToTarget(nextBlock.NBits)

	// Perform target adjustment calculation
	prevInt := new(big.Int).SetBytes(prevTarget[:])
	ratioFloat := big.NewFloat(ratio)
	newTargetFloat := new(big.Float).Mul(big.NewFloat(0).SetInt(prevInt), ratioFloat)
	newTargetInt, _ := newTargetFloat.Int(nil)
	newTargetBytes := newTargetInt.Bytes()

	// Fit target into 32-byte array
	var newTarget [32]byte
	if len(newTargetBytes) > 32 {
		// Truncate to rightmost 32 bytes if too large
		copy(newTarget[:], newTargetBytes[len(newTargetBytes)-32:])
	} else {
		// Right-align bytes if 32 or fewer
		copy(newTarget[32-len(newTargetBytes):], newTargetBytes)
	}

	// Ensure target doesn't exceed maximum
	if bytes.Compare(newTarget[:], common.MaxTarget[:]) > 0 {
		newTarget = common.MaxTarget
	}

	// Convert target back to nBits
	nBits := common.TargetToNBits(newTarget)

	return newTarget, nBits, nil

}

// Complete verfication steps against given blocks
func verifyBlocks(datFile, dirFile, offFile *os.File, blocks []common.Block) ([]common.Block, []common.Block, []common.Block, error) {
	var verified, failed, orphaned []common.Block

	// Handle genesis block case
	if len(blocks) == 1 && blocks[0].Height == 1 {
		genesis := common.GenesisBlock()
		if genesis.Hash() != blocks[0].Hash() {
			common.PrintToLog("Block 1 failed verification: genesis block check")
			failed = append(failed, blocks[0])
		} else {
			verified = append(verified, blocks[0])
		}
		return verified, failed, orphaned, nil
	}

	// Sort blocks by height, ascending
	sort.Slice(blocks, func(i, j int) bool {
		return blocks[i].Height < blocks[j].Height
	})

	// Batched verification requires continuous heights. Check continuity here
	if len(blocks) > 1 {
		for i := 1; i < len(blocks); i++ {
			if blocks[i].Height == blocks[i-1].Height {
				continue
			}
			if blocks[i].Height != blocks[i-1].Height+1 {
				return nil, nil, nil, fmt.Errorf("discontinuous blocks, cannot verify")
			}
		}
	}

	if blocks[0].Height <= 0 {
		return nil, nil, nil, fmt.Errorf("unsupported block height %v", blocks[0].Height)
	}

	// Calculate cutoff for initial chain blocks
	cutoff := 0
	if maxAB(0, blocks[0].Height-11) == 0 {
		cutoff = 11 - (blocks[0].Height - 1)
	}

	// Fetch last 11 blocks from chain
	fileHeights := make([]int, 0, 11-cutoff)
	for i := maxAB(1, blocks[0].Height-11); i < blocks[0].Height; i++ { // Gather heights
		fileHeights = append(fileHeights, i)
	}
	fileBlocksResp, err := readBlocks(datFile, dirFile, offFile, fileHeights) // Read blocks from file
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to read blocks: %v", err)
	}
	var fileBlocks []common.Block
	for _, group := range fileBlocksResp {
		fileBlocks = append(fileBlocks, group...) // flatten the response
	}

	// Prepare blocks for verification
	type pendingBlock struct {
		isSet    bool
		block    common.Block
		verified bool
		original bool
		hash     [32]byte
	}

	pending := make([]pendingBlock, 0, len(fileBlocks)+len(blocks))
	for _, b := range fileBlocks {
		pending = append(pending, pendingBlock{block: b, verified: true, original: true, hash: b.Hash()})
	}
	for _, b := range blocks {
		pending = append(pending, pendingBlock{block: b, verified: false, original: false, hash: b.Hash()})
	}

	// Verify each non-original block
	for i, pb := range pending {
		if pb.original { // Skip originals
			continue
		}

		// Difficulty check
		target := common.NBitsToTarget(pb.block.NBits)
		if bytes.Compare(pb.hash[:], target[:]) >= 0 {
			common.PrintToLog(fmt.Sprintf("block %d failed difficulty check", pb.block.Height))
			failed = append(failed, pb.block)
			continue
		}

		// PrevHash & Height Check
		var prevBlock pendingBlock
		for _, candidate := range pending {
			if candidate.verified && candidate.hash == pb.block.PrevHash {
				prevBlock = candidate
				prevBlock.isSet = true
				break
			}
		}
		if !prevBlock.isSet {
			common.PrintToLog(fmt.Sprintf("block %d is orphaned", pb.block.Height))
			orphaned = append(orphaned, pb.block)
			continue
		}
		if prevBlock.block.Height+1 != pb.block.Height {
			common.PrintToLog(fmt.Sprintf("block %d failed height check", pb.block.Height))
			failed = append(failed, pb.block)
			continue
		}

		// Timestamp check 1: check against median of last 11 blocks
		timestamps := make([]int64, 0, 11-cutoff)
		refBlock := pb.block
		for len(timestamps) < 11-cutoff {
			found := false
			for _, prev := range pending {
				if prev.verified && prev.hash == refBlock.PrevHash {
					timestamps = append(timestamps, prev.block.Timestamp)
					refBlock = prev.block
					found = true
					break
				}
			}
			if !found {
				return nil, nil, nil, fmt.Errorf("failed to locate prevHash chain for block %d at reference block %d",
					pb.block.Height, refBlock.Height)
			}
		}
		if pb.block.Timestamp < medianTimestamp(timestamps) { // Make this "<=" for final version to limit stagnation
			common.PrintToLog(fmt.Sprintf("failed verification: timestamp check #1 at block %d | timestamp is %v | median is %v", pb.block.Height, pb.block.Timestamp, medianTimestamp(timestamps)))
			failed = append(failed, pb.block)
			continue
		}

		// Timestamp check 2: < now + 2 minutes
		maxTime := time.Now().UnixMilli() + 120000
		if pb.block.Timestamp > maxTime {
			common.PrintToLog(fmt.Sprintf("failed verification: timestamp check #2 at block %d | timestamp is %v | maxTime is %v", pb.block.Height, pb.block.Timestamp, maxTime))
			failed = append(failed, pb.block)
			continue
		}

		verified = append(verified, pb.block)
		pending[i].verified = true

	}

	return verified, failed, orphaned, nil
}

func calculateRatio(block common.Block) (float64, error) {

	// Gather block heights of last AdjustmentInterval
	heights := make([]int, 0, adjustmentInterval+1)
	for i := block.Height - (adjustmentInterval + 1); i < block.Height; i++ {
		heights = append(heights, i)
	}
	common.PrintToLog(fmt.Sprintf("Requested %v heights | first %v, last %v",
		len(heights), heights[0], heights[len(heights)-1]))

	unfilteredBlocks := common.RequestBlocks(heights)
	if len(heights) != len(unfilteredBlocks) {
		return 0, fmt.Errorf("mismatch between requested heights and returned blocks")
	}

	// Locate the given blocks chain
	currentBlocks := []common.Block{block}
	prevBlocks, err := filterBlocks(unfilteredBlocks, currentBlocks)
	if err != nil {
		return 0, fmt.Errorf("block filtering failed: %v", err)
	}
	common.PrintToLog(fmt.Sprintf("filtered %d blocks | first height %v, last height %v",
		len(prevBlocks), prevBlocks[0][0].Height, prevBlocks[len(prevBlocks)-1][0].Height))

	// Calculate average block time
	firstTimestamp := prevBlocks[len(prevBlocks)-1][0].Timestamp
	timeSpan := prevBlocks[0][0].Timestamp - firstTimestamp
	avgBlockTime := float64(timeSpan) / float64(adjustmentInterval)

	// Compute difficulty adjustment ratio
	ratio := float64(timeSpan) / float64(targetPeriodTime)
	common.PrintToLog(fmt.Sprintf("Average block time: %.2f | Ratio: %.2f", avgBlockTime/1000, ratio))

	// Clamp ratio within consensus bounds
	if ratio < minDifficultyRatio {
		ratio = minDifficultyRatio
	}
	if ratio > maxDifficultyRatio {
		ratio = maxDifficultyRatio
	}

	return ratio, nil
}

func medianTimestamp(ts []int64) int64 {
	sort.Slice(ts, func(i, j int) bool { return ts[i] < ts[j] })
	n := len(ts)
	if n == 0 {
		return 0
	}
	if n%2 == 0 {
		return (ts[n/2-1] + ts[n/2]) / 2
	}
	return ts[n/2]
}

func maxAB(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// filterBlocks filters groups of blocks by height and hash continuity, ensuring they form a valid chain
// Returns: Filtered block groups in descending height order, or and error if gaps or invalid links are found
func filterBlocks(blockGroups [][]common.Block, referenceBlocks []common.Block) ([][]common.Block, error) {

	// Create a copy to avoid modifying the input
	sortedGroups := make([][]common.Block, len(blockGroups))
	copy(sortedGroups, blockGroups)

	// Sort groups by the height of their first block (descending order)
	sort.Slice(sortedGroups, func(i, j int) bool {
		return sortedGroups[i][0].Height > sortedGroups[j][0].Height // Decreasing order
	})

	// Verify no height gaps exist between groups
	for i := 1; i < len(sortedGroups); i++ {
		if sortedGroups[i-1][0].Height-sortedGroups[i][0].Height != 1 {
			return nil, fmt.Errorf("height gap detected between %d and %d",
				sortedGroups[i-1][0].Height, sortedGroups[i][0].Height)
		}
	}

	// Initialize reference hashes for chain validation
	refHashes := make(map[[32]byte]bool)
	isReferenceNil := referenceBlocks == nil
	if isReferenceNil {
		// Use hashes of blocks in the hightes height group as initial references
		for _, block := range sortedGroups[0] {
			refHashes[block.Hash()] = true
		}
	} else {
		// Use PrevHash from reference blocks to validate the next group
		for _, ref := range referenceBlocks {
			refHashes[ref.PrevHash] = true
		}
	}

	// Filter blocks by matching hashes to reference hashes
	filteredGroups := make([][]common.Block, 0, len(sortedGroups))
	for i, group := range sortedGroups {
		filteredGroup := make([]common.Block, 0)
		for _, block := range group {
			// Include block if its hash matches a reference hash or if it's in the first group with nil references
			if refHashes[block.Hash()] || (isReferenceNil && i == 0) {
				filteredGroup = append(filteredGroup, block)
			}
		}

		// Validate and store filtered group
		if len(filteredGroup) == 0 {
			return nil, fmt.Errorf("no valid blocks at height %d", group[0].Height)
		}
		filteredGroups = append(filteredGroups, filteredGroup)

		// Update reference hashes for the next group (if any)
		if i < len(sortedGroups)-1 {
			refHashes = make(map[[32]byte]bool)
			for _, block := range filteredGroup {
				refHashes[block.PrevHash] = true
			}
		}
	}

	return filteredGroups, nil
}
