package mine

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"pareme/common"
	"pareme/inout"
	"strings"
	"sync"
	"time"
)

// minerManager coordinates mining operations and chain updates
// Returns: Channel for console commands
func MinerManager(ctx context.Context, wg *sync.WaitGroup, newHeightsChan chan []int) chan []string {
	common.PrintToLog("\nInitializing Miner...")

	// Channels for mining coordination
	blockToMineChan := make(chan common.Block)    // Sends blocks to mine
	interruptMiningChan := make(chan struct{}, 1) // Signals mining interruption
	stopMiningChan := make(chan int)              // Signals mining to stop
	minedBlockChan := make(chan common.Block)     // Revieves mined blocks
	consoleChan := make(chan []string)            // Connect with console

	var hashesToMine []string
	newHeights := make([]int, 0) // Tracks new block heights

	// Start the mining goroutine
	go mining(blockToMineChan, minedBlockChan, interruptMiningChan, stopMiningChan)

	// Manage mining process
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(blockToMineChan)
		defer close(minedBlockChan)
		defer close(consoleChan)

		for {
			select {
			case <-ctx.Done():
				// Shutdown
				if common.MiningState.IsFinding {
					interruptMiningChan <- struct{}{}
					block := <-minedBlockChan
					if block.Nonce == -1 {
						common.PrintToLog("Mining interrupted")
					} else { // rare but possible
						common.PrintToLog("Mining completed block before stopping")
					}
				} else {
					stopMiningChan <- 1
				}
				common.PrintToLog("Miner shutting down")
				return

			case consoleReq := <-consoleChan:
				// Handle console commands
				if len(consoleReq) != 0 {
					if common.MiningState.Active {
						common.PrintToLog("Already Mining!")
					} else {
						// Start mining
						hashesToMine = consoleReq
						currentHeight, _ := common.RequestChainStats()
						nextBlock, err := buildBlock(currentHeight, hashesToMine)
						if err != nil {
							common.PrintToLog(fmt.Sprintf("Failed to build next block: %v", err))
							continue
						}

						common.PrintToLog(fmt.Sprintf("\nStarting miner at Block %d", nextBlock.Height))
						common.MiningState.IsFinding = true
						blockToMineChan <- nextBlock

						common.MiningState.Active = true
						common.MiningState.Height = nextBlock.Height
					}
				} else {
					if !common.MiningState.Active {
						common.PrintToLog("Already Not Mining!")
					} else {
						// Stop mining
						if common.MiningState.IsFinding {
							interruptMiningChan <- struct{}{}
							<-minedBlockChan

							common.MiningState.Active = false
							common.MiningState.Height = 0
						} else {
							common.MiningState.Active = false
							common.MiningState.Height = 0
						}
					}
				}

			case block := <-minedBlockChan:
				// Mined block; send to writer
				hash := block.Hash()
				common.PrintToLog(fmt.Sprintf("Mined Block %d with Hash: %x | ID: %x", block.Height, hash[:8], hash[30:]))
				writeBlockReq := common.WriteBlockRequest{
					Blocks: []common.Block{block},
					Type:   "mined",
					From:   nil,
				}
				common.WriteBlockChan <- writeBlockReq // Send to writer

			case heights := <-newHeightsChan:
				// Handle new heights
				newHeights = append(newHeights, heights...)
				maxHeight := max(newHeights)
				common.PrintToLog(fmt.Sprintf("newHeights: %v", newHeights))
				if !common.MiningState.Active {
					continue
				}

				if maxHeight > common.MiningState.Height+2 {
					// Chain is 2+ blocks ahead; interrupt and discard current mining
					common.PrintToLog(fmt.Sprintf("Chain is ahead: %d vs miner: %d; stopping", maxHeight, common.MiningState.Height))
					if common.MiningState.IsFinding {
						interruptMiningChan <- struct{}{}
						<-minedBlockChan
						common.MiningState.IsFinding = false
					}
				}
				if !common.MiningState.IsFinding {
					nextBlock, err := buildBlock(maxHeight, hashesToMine)
					if err != nil {
						common.PrintToLog(fmt.Sprintf("Failed to build block: %v", err))
						continue
					}
					common.PrintToLog(fmt.Sprintf("\n----- Starting mining on block %d at %v -----", nextBlock.Height, time.Now()))
					common.MiningState.IsFinding = true
					blockToMineChan <- nextBlock
					common.MiningState.Height = maxHeight
					newHeights = nil
				}
			}
		}
	}()
	return consoleChan
}

// mining runs a loop to process blocks for mining
func mining(blockToMineChan <-chan common.Block, minedBlockChan chan<- common.Block, interruptMiningChan <-chan struct{}, stopMiningChan <-chan int) {
	for {
		select {
		case block := <-blockToMineChan:
			// Mine block
			nonce := findNonce(block, interruptMiningChan)
			block.Nonce = nonce
			minedBlockChan <- block // Send mined block back
			common.MiningState.IsFinding = false
		case <-stopMiningChan:
			return
		}
	}

}

// findNonce searches for a nonce that satifies the block's difficulty
func findNonce(b common.Block, interruptChan <-chan struct{}) int {
	start := time.Now().Unix()
	blockDifficulty := common.NBitsToTarget(b.NBits)
	common.PrintToLog(fmt.Sprintf("Finding Nonce for Block %d, Difficulty: %x", b.Height, blockDifficulty[:8]))

	for i := 1; i <= common.MaxNonce; i++ {
		if i%1000 == 0 {
			//common.PrintToLog(fmt.Sprintf("Current hash: %v", b.Hash()))
			select {
			case <-interruptChan:
				common.PrintToLog(fmt.Sprintf("Nonce search interrupted for Block %d on nonce %v", b.Height, i))
				return -1
			default:
			}
		}
		b.Nonce = i
		hash := b.Hash()
		if bytes.Compare(hash[:], blockDifficulty[:]) < 0 {
			common.PrintToLog(fmt.Sprintf("Nonce %d found in %d seconds", b.Nonce, time.Now().Unix()-start))
			return b.Nonce
		}
	}

	common.PrintToLog(fmt.Sprintf("No nonce found in %d seconds", time.Now().Unix()-start))
	return -1
}

// buildBlock constructs a new block for mining based on the current height
func buildBlock(height int, hashesToMine []string) (common.Block, error) {
	var currentBlock common.Block
	if height == 1 {
		currentBlock = common.GenesisBlock()
	} else {
		currentBlock = common.RequestBlocks([]int{height})[0][0]
	}
	prevHash := currentBlock.Hash()

	hashesToMineBytes := make([][32]byte, 0, len(hashesToMine))

	for _, hash := range hashesToMine {
		hashBytes, err := hashStringToByte32(hash)
		if err != nil {
			return common.Block{}, fmt.Errorf("failed to convert hash to bytes: %v", err)
		}

		hashesToMineBytes = append(hashesToMineBytes, hashBytes)
	}

	/*
		hashToMineBytes, err := hashStringToByte32(hashToMine)
		if err != nil {
			return common.Block{}, fmt.Errorf("failed to convert hash to bytes: %v", err)
		}
	*/

	nextBlock, err := common.NewBlock(height+1, prevHash, currentBlock.NBits, hashesToMineBytes)
	if err != nil {
		return common.Block{}, fmt.Errorf("failed to create newBlock: %v", err)
	}

	if (height)%2016 == 0 && height != 2016 { // Skip first ever adjustment
		// Adjust difficulty every 2016 blocks (except genesis)
		//nextBlock.Difficulty = adjustDifficulty(nextBlock)
		_, nBits, err := inout.CalculateDifficulty(nextBlock)
		if err != nil {
			return common.Block{}, fmt.Errorf("failed to determine difficulty: %v", err)
		}
		nextBlock.NBits = nBits
	}
	return nextBlock, nil
}

func hashStringToByte32(hashStr string) ([32]byte, error) {
	// Remove any "0x" prefix if present
	hashStr = strings.TrimPrefix(hashStr, "0x")

	// Decode the hex string into a byte slice
	hashBytes, err := hex.DecodeString(hashStr)
	if err != nil {
		return [32]byte{}, fmt.Errorf("failed to decode hex string: %v", err)
	}

	// Check if the length is exactly 32 bytes
	if len(hashBytes) != 32 {
		return [32]byte{}, fmt.Errorf("hash length must be 32 bytes, got %d", len(hashBytes))
	}

	// Convert slice to fixed-size array
	var result [32]byte
	copy(result[:], hashBytes)
	return result, nil
}

// max returns the maximum value in a slice of integers
func max(heights []int) int {
	if len(heights) == 0 {
		return 0
	}
	maxH := heights[0]
	for _, h := range heights {
		if h > maxH {
			maxH = h
		}
	}
	return maxH
}
