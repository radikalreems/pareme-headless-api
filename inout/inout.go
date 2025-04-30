package inout

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"pareme/common"
	"pareme/network"
	"path/filepath"
	"sync"
)

const (
	//blocksDir    = "blocks"         // Directory for block data
	//dirIndexFile = "dir0000.idx"    // Directory index file
	//offIndexFile = "off0000.idx"    // Offset index file
	//dataFile     = "pareme0000.dat" // Block data file
	offEntrySize = 5      // Size of entries in offset file
	dirEntrySize = 4      // Size of entries in directory file
	MagicBytes   = "PARE" // Magic bytes embeded before every block entry
	MagicSize    = 4      // Size of magic bytes
	dirPerm      = 0755
	filePerm     = 0666
	dataFilePerm = 0644
)

/*
// syncChain synchronizes the blockchain from files
// Returns: Error if synchronization fails, nil otherwise
func SyncChain() error {
	common.PrintToLog("\nSyncing blockchain from files...")

	// Init blockchain files
	err := initFiles() // Set up folder, dat, index file
	if err != nil {
		return fmt.Errorf("failed syncing files: %v", err)
	}

	// Get file stats for index and data

	dirPath := filepath.Join(common.BlocksDir, common.DirIndexFile)
	dirStat, err := os.Stat(dirPath)
	if err != nil {
		return fmt.Errorf("failed stating index file %s: %v", dirPath, err)
	}
	datPath := filepath.Join(common.BlocksDir, common.DataFile)
	datStat, err := os.Stat(datPath)
	if err != nil {
		return fmt.Errorf("failed stating data file %s: %v", datPath, err)
	}


	// Calculate blockchain height and total blocks
	if dirStat.Size()%indexEntrySize != 0 {
		return fmt.Errorf("invalid index file size: %d bytes", dirStat.Size())
	}
	height := int(dirStat.Size() / indexEntrySize)
	if datStat.Size()%blockEntrySize != 0 {
		return fmt.Errorf("invalid data file size: %d bytes", datStat.Size())
	}
	totalBlocks := int(datStat.Size() / blockEntrySize)



	common.PrintToLog(fmt.Sprintf("Synced to height %d with %d total blocks", height, totalBlocks))
	return nil
}
*/

// blockWriter processes incoming blocks and read requests, updating the blockchain files
// Returns: Channel for new block heights, error if setup fails
func BlockWriter(ctx context.Context, wg *sync.WaitGroup) (chan []int, error) {
	common.PrintToLog("\nStarting up blockWriter...")

	// Open DAT file
	datFilePath := filepath.Join(common.BlocksDir, common.DataFile)
	datFile, err := os.OpenFile(datFilePath, os.O_APPEND|os.O_RDWR, dataFilePerm)
	if err != nil {
		return nil, fmt.Errorf("failed to open DAT file %s: %v", datFilePath, err)
	}

	// Open DIR file
	dirFilePath := filepath.Join(common.BlocksDir, common.DirIndexFile)
	dirFile, err := os.OpenFile(dirFilePath, os.O_RDWR, filePerm)
	if err != nil {
		datFile.Close()
		return nil, fmt.Errorf("failed to open DIR file %s: %v", dirFilePath, err)
	}

	// Open OFF file
	offFilePath := filepath.Join(common.BlocksDir, common.OffIndexFile)
	offFile, err := os.OpenFile(offFilePath, os.O_RDWR, filePerm)
	if err != nil {
		datFile.Close()
		dirFile.Close()
		return nil, fmt.Errorf("failed to open OFF file %s: %v", offFilePath, err)
	}

	// Channel for notifying miners
	newHeightsChan := make(chan []int, 10) // Send new blocks to miner

	// Process blocks and requests
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer datFile.Close()
		defer dirFile.Close()
		defer offFile.Close()

		orphaned := make([]common.Block, 0)

		for {
			select {
			case <-ctx.Done():
				common.PrintToLog("Block writer shutting down")
				return

			case respChan := <-common.IndexRequestChan:
				common.RWMu.Lock()
				// Handle chain stats request
				height, totalBlocks, err := readChainStats(dirFile, offFile)
				if err != nil {
					common.PrintToLog(fmt.Sprintf("Error retreiving chain stats: %v", err))
				}
				respChan <- [2]uint32{uint32(height), uint32(totalBlocks)}
				close(respChan)
				common.RWMu.Unlock()

			case req := <-common.RequestChan:
				common.RWMu.Lock()
				// Handle block read request
				response, err := readBlocks(datFile, dirFile, offFile, req.Heights)
				if err != nil {
					common.PrintToLog(fmt.Sprintf("Error reading blocks for heights %v: %v", req.Heights, err))
				}
				req.Response <- response
				close(req.Response)
				common.RWMu.Unlock()

			case writeBlockReq := <-common.WriteBlockChan:
				common.RWMu.Lock()
				// Verify and write new blocks
				common.PrintToLog(fmt.Sprintf("\nwriter: Recieved %d new blocks. verifying...", len(writeBlockReq.Blocks)))
				blocksToVerify := make([]common.Block, 0, len(writeBlockReq.Blocks)+len(orphaned))
				blocksToVerify = append(blocksToVerify, writeBlockReq.Blocks...)
				blocksToVerify = append(blocksToVerify, orphaned...)
				verified, failed, orphans, err := verifyBlocks(datFile, dirFile, offFile, blocksToVerify)
				if err != nil {
					common.PrintToLog(fmt.Sprintf("Error verifying blocks: %v", err))
				}
				common.PrintToLog(fmt.Sprintf("%d verified | %d failed | %d orphaned", len(verified), len(failed), len(orphans)))
				orphaned = orphans
				if len(verified) == 0 {
					continue
				}
				err = writeBlocks(datFile, dirFile, offFile, verified)
				if err != nil {
					common.PrintToLog(fmt.Sprintf("failed to write %d blocks: %v", len(verified), err))
					continue
				}
				common.PrintToLog(fmt.Sprintf("Successfully wrote %d new blocks!", len(verified)))

				// Broadcast to peers
				var exclusion []*common.Peer
				if writeBlockReq.Type == "sync" {
					for i := range common.AllPeers {
						exclusion = append(exclusion, common.AllPeers[i])
					}
				} else if writeBlockReq.Type == "received" || writeBlockReq.Type == "mined" {
					exclusion = []*common.Peer{writeBlockReq.From}
				}

				for _, block := range verified {
					go network.BroadcastBlock(block, exclusion)
				}

				// Notify miners
				if common.MiningState.Active {
					heights := make([]int, 0, len(verified))
					for _, v := range verified {
						heights = append(heights, v.Height)
					}
					newHeightsChan <- heights
				}

				common.RWMu.Unlock()

			}
		}
	}()

	return newHeightsChan, nil
}

//---------------- DIRECT I/O FUNCTIONS

// InitFiles initializes data files (.dat and .idx)
// Returns: Error if initialization fails, nil otherwise
func InitFiles() error {
	common.PrintToLog("Initializing files")

	// Ensure blocks directory exists
	if err := os.MkdirAll(common.BlocksDir, dirPerm); err != nil {
		return fmt.Errorf("failed creating directory %s: %v", common.BlocksDir, err)
	}

	// Initialize DAT file
	datFilePath := filepath.Join(common.BlocksDir, common.DataFile)
	datFile, err := os.OpenFile(datFilePath, os.O_CREATE|os.O_APPEND|os.O_RDWR, filePerm)
	if err != nil {
		return fmt.Errorf("failed creating %s: %v", datFilePath, err)
	}
	defer datFile.Close()

	// Initialize DIR file (wipe if exists)
	dirFilePath := filepath.Join(common.BlocksDir, common.DirIndexFile)
	dirFile, err := os.OpenFile(dirFilePath, os.O_CREATE|os.O_RDWR, filePerm)
	//dirFile, err := os.Create(dirFilePath)
	if err != nil {
		return fmt.Errorf("failed creating %s: %v", dirFilePath, err)
	}
	defer dirFile.Close()

	// Initialize OFF file (wipe if exists)
	offFilePath := filepath.Join(common.BlocksDir, common.OffIndexFile)
	offFile, err := os.OpenFile(offFilePath, os.O_CREATE|os.O_RDWR, filePerm)
	//offFile, err := os.Create(offFilePath)
	if err != nil {
		return fmt.Errorf("failed creating %s: %v", offFilePath, err)
	}
	defer offFile.Close()

	// Determine if first startup
	datFileStat, err := datFile.Stat()
	if err != nil {
		return fmt.Errorf("failed stating %s: %v", datFilePath, err)
	}
	if datFileStat.Size() == 0 {
		// Write genesis block
		blocks := []common.Block{common.GenesisBlock()}
		err := writeBlocks(datFile, dirFile, offFile, blocks)
		if err != nil {
			return fmt.Errorf("failed to write genesis: %v", err)
		}
		common.PrintToLog(fmt.Sprintf("Created %s with genesis", datFilePath))

		// init index files
		err = os.Truncate(dirFilePath, 0)
		if err != nil {
			return fmt.Errorf("failed to truncate DIR file: %v", err)
		}
		err = os.Truncate(offFilePath, 0)
		if err != nil {
			return fmt.Errorf("failed to truncate OFF file: %v", err)
		}

		_, err = dirFile.Seek(0, io.SeekStart)
		if err != nil {
			return fmt.Errorf("failed to seek DIR file: %v", err)
		}
		err = binary.Write(dirFile, binary.BigEndian, uint32(1))
		if err != nil {
			return fmt.Errorf("failed to write the init of DIR file: %v", err)
		}

		_, err = offFile.Seek(0, io.SeekStart)
		if err != nil {
			return fmt.Errorf("failed to seek OFF file: %v", err)
		}
		err = binary.Write(offFile, binary.BigEndian, []byte{0, 0, 0, 0, 0})
		if err != nil {
			return fmt.Errorf("failed to write the init of OFF file: %v", err)
		}
	}

	/*
		// Initialize DIR and OFF files based on DAT
		err = initIndexFiles(datFile, dirFile, offFile)
		if err != nil {
			return fmt.Errorf("failed to init index files: %v", err)
		}
	*/

	// Read chain stats to verify initialization
	height, totalBlocks, err := readChainStats(dirFile, offFile)
	if err != nil {
		return fmt.Errorf("failed to read chain stats: %v", err)
	}
	common.PrintToLog(fmt.Sprintf("Synced to height %d with %d total blocks", height, totalBlocks))

	return nil

}

/*
// initIndexFiles syncs DIR and OFF index files with DAT file
// Returns: Error if initialization fails, nil otherwise
func initIndexFiles(datFile, directoryFile, offsetFile *os.File) error {
	common.PrintToLog("Syncing index files to dat...")

	// Get DAT file stats
	datStat, err := datFile.Stat()
	if err != nil {
		return fmt.Errorf("failed to get DAT file stats: %v", err)
	}
	if datStat.Size()%blockEntrySize != 0 {
		return fmt.Errorf("invalid DAT file size: %d bytes", datStat.Size())
	}
	numBlocks := datStat.Size() / int64(blockEntrySize)
	common.PrintToLog(fmt.Sprintf("%d blocks in DAT", numBlocks))

	// Group offsets by height
	heightToOffsets := make(map[int][]uint32)
	for offset := uint32(0); offset < uint32(numBlocks); offset++ {
		// Seek to the block start
		_, err := datFile.Seek(int64(offset*uint32(blockEntrySize)), 0)
		if err != nil {
			return fmt.Errorf("failed to seek to offset %d in DAT: %v", offset, err)
		}

		// Verify magic bytes
		magicCheck := make([]byte, MagicSize)
		_, err = datFile.Read(magicCheck)
		if err != nil {
			return fmt.Errorf("failed to read magic byte at offset %d: %v", offset, err)
		}
		if string(magicCheck) != MagicBytes {
			return fmt.Errorf("invalid magic bytes at offset %d: got %v, want %v", offset, magicCheck, MagicBytes)
		}

		// Read height
		heightBytes := make([]byte, 4)
		_, err = datFile.Read(heightBytes)
		if err != nil {
			return fmt.Errorf("failed to read height at offset %d: %v", offset, err)
		}
		height := int(binary.BigEndian.Uint32(heightBytes))

		// Store offset for height
		heightToOffsets[height] = append(heightToOffsets[height], offset)
	}

	// Sort heights
	heights := make([]int, 0, len(heightToOffsets))
	for height := range heightToOffsets {
		heights = append(heights, height)
	}
	sort.Ints(heights)

	// Build offsets and directory
	offsets := make([]uint32, 0, numBlocks)
	directory := make([]uint32, 0, len(heights))
	for _, height := range heights {
		offsets = append(offsets, heightToOffsets[height]...)
		directory = append(directory, uint32(len(offsets)))
	}

	// Log last 8 entries for debugging
	if len(directory) < 9 {
		common.PrintToLog(fmt.Sprintf("Directory values: %v", directory))
	} else {
		common.PrintToLog(fmt.Sprintf("Directory values: %v", directory[len(directory)-8:]))
	}
	if len(offsets) < 9 {
		common.PrintToLog(fmt.Sprintf("Offset values: %v", offsets))
	} else {
		common.PrintToLog(fmt.Sprintf("Offset values: %v", offsets[len(offsets)-8:]))
	}

	// Write OFF file
	if err := offsetFile.Truncate(0); err != nil {
		return fmt.Errorf("failed to truncate OFF file: %v", err)
	}
	if _, err := offsetFile.Seek(0, 0); err != nil {
		return fmt.Errorf("failed to seek OFF file: %v", err)
	}
	for _, offset := range offsets {
		if err := binary.Write(offsetFile, binary.BigEndian, offset); err != nil {
			return fmt.Errorf("failed to write to offset %d to OFF file: %v", offset, err)
		}
	}

	// Write DIR file
	if err := directoryFile.Truncate(0); err != nil {
		return fmt.Errorf("failed to truncate DIR file: %v", err)
	}
	if _, err := directoryFile.Seek(0, 0); err != nil {
		return fmt.Errorf("failed to seek DIR file: %v", err)
	}
	for _, endPos := range directory {
		if err := binary.Write(directoryFile, binary.BigEndian, endPos); err != nil {
			return fmt.Errorf("failed to write end position %d to DIR file: %v", endPos, err)
		}
	}

	return nil
}
*/

// updateIndexFiles updates DIR and OFF files for new blocks
// Returns: Error if update fails, nil otherwise
func updateIndexFiles(datFile, directoryFile, offsetFile *os.File, newBlocks []common.Block) error {
	if len(newBlocks) == 0 {
		return fmt.Errorf("no new blocks given")
	}

	// Determine the offset where the new blocks were written
	blockSizes := make([]int, 0, len(newBlocks))
	var bytesAdded int
	for _, block := range newBlocks {
		blockSizes = append(blockSizes, MagicSize+len(block.ToByte()))
		bytesAdded += (MagicSize + len(block.ToByte()))
	}
	datStat, err := datFile.Stat()
	if err != nil {
		return fmt.Errorf("failed to data DAT file: %v", err)
	}
	startOffset := int(datStat.Size()) - bytesAdded

	offset := startOffset
	for i, newBlock := range newBlocks {
		if i != 0 {
			offset = offset + blockSizes[i-1]
		}

		// Update DIR file
		dirOffset := (newBlock.Height - 1) * dirEntrySize
		dirStat, err := directoryFile.Stat()
		if err != nil {
			return fmt.Errorf("failed to stat DIR file: %v", err)
		}
		if dirStat.Size() == 0 {
			return fmt.Errorf("failed to update DIR file: DIR is empty")
		}

		// Extend DIR file if needed
		if dirStat.Size() < int64(dirOffset+dirEntrySize) {
			// Determine last value
			lastValue := uint32(0)
			_, err = directoryFile.Seek(-dirEntrySize, io.SeekEnd)
			if err != nil {
				return fmt.Errorf("failed to seek DIR file: %v", err)
			}
			err = binary.Read(directoryFile, binary.BigEndian, &lastValue)
			if err != nil {
				return fmt.Errorf("failed to read DIR file: %v", err)
			}

			// Fill added indexes with last value
			for dirStat.Size() < int64(dirOffset+dirEntrySize) {
				// Write last value
				err = binary.Write(directoryFile, binary.BigEndian, lastValue)
				if err != nil {
					return fmt.Errorf("failed to write to DIR file: %v", err)
				}
				// Re-stat directory file with new addition
				dirStat, err = directoryFile.Stat()
				if err != nil {
					return fmt.Errorf("failed to stat DIR file: %v", err)
				}
			}
		}

		// Read and increment DIR value
		_, err = directoryFile.Seek(int64(dirOffset), io.SeekStart)
		if err != nil {
			return fmt.Errorf("failed to seek DIR file to %d: %v", dirOffset, err)
		}
		var offOffsetMultiple uint32
		err = binary.Read(directoryFile, binary.BigEndian, &offOffsetMultiple)
		if err != nil {
			return fmt.Errorf("failed to read DIR file at %d: %v", dirOffset, err)
		}
		offOffset := int64(offOffsetMultiple * offEntrySize)

		// Write incremented DIR value
		_, err = directoryFile.Seek(int64(dirOffset), io.SeekStart)
		if err != nil {
			return fmt.Errorf("failed to seek DIR file to %d: %v", dirOffset, err)
		}
		err = binary.Write(directoryFile, binary.BigEndian, offOffsetMultiple+1)
		if err != nil {
			return fmt.Errorf("failed to write to DIR file at %d: %v", dirOffset, err)
		}

		// Increment subsequent DIR values
		for pos := int64(dirOffset + dirEntrySize); ; pos += int64(dirEntrySize) {
			dirStat, err := directoryFile.Stat()
			if err != nil {
				return fmt.Errorf("failed to stat DIR file: %v", err)
			}
			if pos >= dirStat.Size() {
				break
			}
			_, err = directoryFile.Seek(pos, io.SeekStart)
			if err != nil {
				return fmt.Errorf("failed to seek DIR file to %d: %v", pos, err)
			}
			var temp uint32
			err = binary.Read(directoryFile, binary.BigEndian, &temp)
			if err != nil {
				return fmt.Errorf("failed to read DIR file at %d: %v", pos, err)
			}
			_, err = directoryFile.Seek(pos, io.SeekStart)
			if err != nil {
				return fmt.Errorf("failed to seek DIR file to %d: %v", pos, err)
			}

			err = binary.Write(directoryFile, binary.BigEndian, temp+1)
			if err != nil {
				return fmt.Errorf("failed to write to DIR file at %d: %v", pos, err)
			}
		}

		// Update OFF file
		offStat, err := offsetFile.Stat()
		if err != nil {
			return fmt.Errorf("failed to stat OFF file: %v", err)
		}
		offSize := offStat.Size()
		if offOffset > offSize {
			return fmt.Errorf("offset %d beyond OFF file size %d", offOffset, offSize)
		}

		if offOffset == offSize {
			// Append to OFF file
			_, err = offsetFile.Seek(0, io.SeekEnd)
			if err != nil {
				return fmt.Errorf("failed to seek OFF file: %v", err)
			}

			buf := make([]byte, 5)
			temp := make([]byte, 8)
			binary.BigEndian.PutUint64(temp, uint64(offset))
			copy(buf, temp[3:8]) // Take the last 5 bytes

			err = binary.Write(offsetFile, binary.BigEndian, buf)
			if err != nil {
				return fmt.Errorf("failed to write offset %d to OFF file: %v", offset, err)
			}
		} else {
			// Shift and insert in OFF file
			buf := make([]byte, 0)
			_, err = offsetFile.Seek(offOffset, io.SeekStart)
			if err != nil {
				return fmt.Errorf("failed to seek OFF file to %d: %v", offOffset, err)
			}
			for {
				temp := make([]byte, 5)
				err := binary.Read(offsetFile, binary.BigEndian, &temp)
				if err == io.EOF {
					break
				}
				if err != nil {
					return fmt.Errorf("failed to read OFF file at %d: %v", offOffset, err)
				}
				buf = append(buf, temp...)
			}

			// Insert new offset
			_, err = offsetFile.Seek(offOffset, io.SeekStart)
			if err != nil {
				return fmt.Errorf("failed to seek OFF file to %d: %v", offOffset, err)
			}
			temp := make([]byte, 8)
			binary.BigEndian.PutUint64(temp, uint64(offset))

			var offsetInBytes [5]byte
			copy(offsetInBytes[:], temp[3:8])

			err = binary.Write(offsetFile, binary.BigEndian, offsetInBytes)
			if err != nil {
				return fmt.Errorf("failed to write offset %d to OFF file: %v", offset, err)
			}

			// Append remaining offsets
			err = binary.Write(offsetFile, binary.BigEndian, buf)
			if err != nil {
				return fmt.Errorf("failed to append remaining offsets after insertion of offset %v", offset)
			}
			/*
				// Append remaining offsets
				for _, v := range buf {
					err = binary.Write(offsetFile, binary.BigEndian, v)
					if err != nil {
						return fmt.Errorf("failed to write offset %d to OFF file: %v", v, err)
					}
				}
			*/
		}
	}

	/*
		// Log updated chain stats
		datSlice, _, _, err := displayIndexFiles(datFile, directoryFile, offsetFile)
		if err != nil {
			return fmt.Errorf("failed to display index files: %v", err)
		}
		// Print updated chain stats but limit to last 8
		if len(datSlice) <= 8 {
			common.PrintToLog(fmt.Sprintf("Dat file: %v", datSlice))
		} else {
			common.PrintToLog(fmt.Sprintf("Dat file: %v", datSlice[len(datSlice)-8:]))
		}


			if len(dirSlice) < 9 {
				common.PrintToLog(fmt.Sprintf("Directory file: %v", dirSlice))
			} else {
				common.PrintToLog(fmt.Sprintf("Directory file: %v", dirSlice[len(dirSlice)-8:]))
			}
			if len(offSlice) < 9 {
				common.PrintToLog(fmt.Sprintf("Offset file: %v", offSlice))
			} else {
				common.PrintToLog(fmt.Sprintf("Offset file: %v", offSlice[len(offSlice)-8:]))
			}
	*/
	return nil
}

// writeBlock appends blocks to the .dat file and updates indexes
// Returns: Error if writing fails, nil otherwise
func writeBlocks(datFile, dirFile, offFile *os.File, blocks []common.Block) error {
	if len(blocks) < 1 {
		return fmt.Errorf("cannot write %d blocks: unsupported amount", len(blocks))
	}

	for _, block := range blocks {
		magic := []byte("PARE")

		blockBytes := block.ToByte()

		blockBytes = append(magic, blockBytes...)

		// Write to DAT file
		if _, err := datFile.Write(blockBytes); err != nil {
			return fmt.Errorf("failed writing block %d: %v", block.Height, err)
		}
		common.PrintToLog(fmt.Sprintf("Wrote Block %d to file. Timestamp: %v secs.", block.Height, block.Timestamp/1000))
	}

	if blocks[0].Height == 1 {
		return nil
	}

	// Update index files
	err := updateIndexFiles(datFile, dirFile, offFile, blocks)
	if err != nil {
		return fmt.Errorf("failed updating index files: %v", err)
	}

	return nil
}

// readBlocks returns blocks requested by height from file
// Returns: Slice of block groups by height, error if reading fails
func readBlocks(datFile, directoryFile, offsetFile *os.File, heights []int) ([][]common.Block, error) {
	if len(heights) == 0 {
		return nil, fmt.Errorf("failed reading blocks: no heights given")
	}

	// Get height group locations
	heightGroupStarts := make([]uint32, 0, len(heights))
	heightGroupSizes := make([]uint32, 0, len(heights))
	for _, height := range heights {
		if height == 1 {
			heightGroupStarts = append(heightGroupStarts, 0)
			heightGroupSizes = append(heightGroupSizes, 1)
			continue
		}
		seekPos := int64((height - 2) * dirEntrySize)
		if _, err := directoryFile.Seek(seekPos, 0); err != nil {
			return nil, fmt.Errorf("failed to seek DIR file to %d: %v", seekPos, err)
		}
		var start, end uint32
		if err := binary.Read(directoryFile, binary.BigEndian, &start); err != nil {
			return nil, fmt.Errorf("failed to read DIR file at %d: %v", seekPos, err)
		}
		if err := binary.Read(directoryFile, binary.BigEndian, &end); err != nil {
			return nil, fmt.Errorf("failed to read DIR file at %d: %v", seekPos+dirEntrySize, err)
		}
		heightGroupStarts = append(heightGroupStarts, start)
		heightGroupSizes = append(heightGroupSizes, end-start)

	}

	// Get block offsets
	blockOffsets := make([][][offEntrySize]byte, len(heights))
	for i, start := range heightGroupStarts {
		if _, err := offsetFile.Seek(int64(start*offEntrySize), 0); err != nil {
			return nil, fmt.Errorf("failed to seek OFF file to %d: %v", start*offEntrySize, err)
		}
		for range heightGroupSizes[i] {
			var offset [offEntrySize]byte
			if err := binary.Read(offsetFile, binary.BigEndian, &offset); err != nil {
				return nil, fmt.Errorf("failed to read OFF file: %v", err)
			}
			blockOffsets[i] = append(blockOffsets[i], offset)
		}
	}
	// Read block
	blocks := make([][]common.Block, len(heights))
	for i, offsets := range blockOffsets {
		for _, offsetByte := range offsets {
			offsetFull := make([]byte, 8)
			copy(offsetFull[3:], offsetByte[:])
			offset := int64(binary.BigEndian.Uint64(offsetFull))
			_, block, err := ReadOneBlock(datFile, offset)
			if err != nil {
				return nil, fmt.Errorf("failed to read one block starting at %v: %v", offset, err)
			}
			blocks[i] = append(blocks[i], block)
		}
	}
	/*
		// Read blocks
		blocks := make([][]common.Block, len(heights))
		for i, offsets := range blockOffsets {
			blockByte := make([]byte, blockEntrySize)
			for _, offset := range offsets {
				if _, err := datFile.Seek(int64(offset*blockEntrySize), 0); err != nil {
					return nil, fmt.Errorf("failed to seek DAT file to %d: %v", offset*blockEntrySize, err)
				}
				n, err := datFile.Read(blockByte)
				if n != blockEntrySize || err != nil {
					return nil, fmt.Errorf("failed to read %d bytes from DAT file at %d: %v", blockEntrySize, offset*blockEntrySize, err)
				}
				if !bytes.Equal(blockByte[:MagicSize], []byte(MagicBytes)) {
					return nil, fmt.Errorf("invalid magic bytes at offset %d", offset)
				}
				block := common.ByteToBlock([common.BlockSize]byte(blockByte[MagicSize:]))
				blocks[i] = append(blocks[i], block)
			}
		}
	*/
	return blocks, nil
}

func ReadOneBlock(datFile *os.File, offset int64) ([]byte, common.Block, error) {
	if _, err := datFile.Seek(offset, io.SeekStart); err != nil {
		return nil, common.Block{}, fmt.Errorf("failed to seek DAT file to offset %v", offset)
	}

	// Check magic
	magic := make([]byte, MagicSize)
	n, err := datFile.Read(magic)
	if err != nil {
		return nil, common.Block{}, fmt.Errorf("failed to read DAT file at offset %v: %v", offset, err)
	}
	if n != MagicSize {
		return nil, common.Block{}, fmt.Errorf("failed to read magic bytes: read %v bytes, expected %v", MagicSize, n)
	}
	if string(magic) != MagicBytes {
		return nil, common.Block{}, fmt.Errorf("failed to verify magic bytes: expected %v, got %v", MagicBytes, string(magic))
	}

	// Read block header
	blockHeader := make([]byte, common.BlockHeaderSize)
	n, err = datFile.Read(blockHeader)
	if err != nil {
		return nil, common.Block{}, fmt.Errorf("failed to read DAT file at offset %v: %v", offset+MagicSize, err)
	}
	if n != common.BlockHeaderSize {
		return nil, common.Block{}, fmt.Errorf("failed to read block header bytes: expected %v bytes, got %v", common.BlockHeaderSize, n)
	}

	// Extract BodyHashSize
	bodyHashSize := (blockHeader[common.BlockHeaderSize-1:])[0]
	bodyHashSizeBytes := int(bodyHashSize * 32)

	// Read Body Hashes
	bodyHashes := make([]byte, bodyHashSizeBytes)
	n, err = datFile.Read(bodyHashes)
	if err != nil {
		return nil, common.Block{}, fmt.Errorf("failed to read DAT file at offset %v: %v", offset+MagicSize+common.BlockHeaderSize, err)
	}
	if n != bodyHashSizeBytes {
		return nil, common.Block{}, fmt.Errorf("failed to read block hashes bytes: expected %v bytes, got %v", bodyHashSizeBytes, n)
	}

	// Verify that the block is finished
	datStat, err := datFile.Stat()
	if err != nil {
		return nil, common.Block{}, fmt.Errorf("failed to stat DAT file: %v", err)
	}
	if datStat.Size() != offset+MagicSize+common.BlockHeaderSize+int64(bodyHashSizeBytes) {
		// Check next magic
		magic := make([]byte, MagicSize)
		n, err := datFile.Read(magic)
		if err != nil {
			return nil, common.Block{}, fmt.Errorf("failed to read DAT file at offset %v: %v", offset+MagicSize+common.BlockHeaderSize+int64(bodyHashSizeBytes), err)
		}
		if n != MagicSize {
			return nil, common.Block{}, fmt.Errorf("failed to read end magic bytes: read %v bytes, expected %v", MagicSize, n)
		}
		if string(magic) != MagicBytes {
			return nil, common.Block{}, fmt.Errorf("failed to verify end magic bytes: expected %v, got %v", MagicBytes, magic)
		}
	}

	// Convert blockBytes to block
	var blockBytes []byte
	blockBytes = append(blockBytes, blockHeader...)
	blockBytes = append(blockBytes, bodyHashes...)
	block, err := common.ByteToBlock(blockBytes)
	if err != nil {
		return nil, common.Block{}, fmt.Errorf("failed to convert bytes to block: %v", err)
	}

	return blockBytes, block, nil

}

// getChainStats returns the latest height and total blocks from file
// Returns: Latest height and total blocks in file, error if reading fails
func readChainStats(directoryFile, offFile *os.File) (int, int, error) {
	// Stat DIR file to determine latest height
	dirStat, err := directoryFile.Stat()
	if err != nil {
		return 0, 0, fmt.Errorf("failed stating DIR file: %v", err)
	}
	height := int(dirStat.Size() / dirEntrySize)

	// Stat OFF file to determine total blocks
	offStat, err := offFile.Stat()
	if err != nil {
		return 0, 0, fmt.Errorf("failed stating OFF file: %v", err)
	}
	totalBlocks := int(offStat.Size() / offEntrySize)

	return height, totalBlocks, nil
}

//--------------- QOL FUNCTIONS

/*
// displayIndexFiles creates a visualization of the files for debugging
func displayIndexFiles(datFile, directoryFile, offsetFile *os.File) ([][2]int, [][2]int, [][2]int, error) {
	readAllUint32 := func(f *os.File) ([]uint32, error) {
		// Seek to start just in case
		if _, err := f.Seek(0, io.SeekStart); err != nil {
			return nil, err
		}

		var values []uint32
		for {
			var v uint32
			err := binary.Read(f, binary.BigEndian, &v)
			if err == io.EOF {
				break
			}
			if err != nil {
				return nil, err
			}
			values = append(values, v)
		}
		return values, nil
	}

	readAllBlocks := func(f *os.File) ([]common.Block, error) {
		if _, err := f.Seek(0, io.SeekStart); err != nil {
			return nil, err
		}

		fileStat, err := f.Stat()
		if err != nil {
			return nil, err
		}
		fileSize := fileStat.Size()
		if fileSize%(int64(common.BlockSize)+4) != 0 {
			return nil, fmt.Errorf("fileSize not a multiple of 88")
		}

		numOfBlocks := int(fileSize / (int64(common.BlockSize) + 4))
		var values [][common.BlockSize]byte
		for i := 0; i < numOfBlocks; i++ {
			_, err := f.Seek(4, io.SeekCurrent)
			if err == io.EOF {
				break
			}
			if err != nil {
				return nil, err
			}
			var v [common.BlockSize]byte
			err = binary.Read(f, binary.BigEndian, &v)
			if err != nil {
				return nil, err
			}
			values = append(values, v)
		}
		var blocks []common.Block
		for i := 0; i < len(values); i++ {
			blocks = append(blocks, common.ByteToBlock(values[i]))
		}
		return blocks, nil
	}

	a1, err1 := readAllBlocks(datFile)
	if err1 != nil {
		return nil, nil, nil, err1
	}
	var idxDat [][2]int
	for a := range a1 {
		internal := a1[a].Height
		idxDat = append(idxDat, [2]int{a, internal})
	}

	a2, err2 := readAllUint32(directoryFile)
	if err2 != nil {
		return nil, nil, nil, err2
	}
	var idxDir [][2]int
	for a := range a2 {
		internal := int(a2[a])
		idxDir = append(idxDir, [2]int{a, internal})
	}

	a3, err3 := readAllUint32(offsetFile)
	if err3 != nil {
		return nil, nil, nil, err3
	}

	var idxOff [][2]int
	for a := range a3 {
		internal := int(a3[a])
		idxOff = append(idxOff, [2]int{a, internal})
	}

	return idxDat, idxDir, idxOff, nil
}
*/
