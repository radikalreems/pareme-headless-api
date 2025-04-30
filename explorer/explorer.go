package explorer

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"pareme/common"
	"pareme/inout"
	"path/filepath"
	"sync"
	"time"

	"github.com/syndtr/goleveldb/leveldb"
)

const (
	dbPath  = "explorer/db"
	apiAddr = "localhost:8000"
)

var (
	syncKey = []byte{0}
	dbMu    sync.RWMutex
)

func StatsManager(ctx context.Context, wg *sync.WaitGroup) error {
	db, err := createDB()
	if err != nil {
		return fmt.Errorf("failed to create DB: %v", err)
	}

	// Open DAT file
	datFilePath := filepath.Join(common.BlocksDir, common.DataFile)
	datFile, err := os.OpenFile(datFilePath, os.O_APPEND|os.O_RDWR, 0644)
	if err != nil {
		return fmt.Errorf("failed to open DAT file %s: %v", datFilePath, err)
	}

	ticker := time.NewTicker(10 * time.Second)

	wg.Add(1)
	go func() {
		defer wg.Done()

		for {
			select {
			case <-ticker.C:
				err := updateDB(datFile, db)
				if err != nil {
					common.PrintToLog(fmt.Sprintf("failed to update DB: %v", err))
					return
				}
			case <-ctx.Done():
				common.PrintToLog("Closing Stats Manager...")
				return
			}
		}

	}()

	err = startAPI(ctx, wg, db, apiAddr)
	if err != nil {
		return fmt.Errorf("failed to start API server")
	}

	return nil
}

func createDB() (*leveldb.DB, error) {
	dbMu.Lock()
	defer dbMu.Unlock()
	// Create directory if it doesn't exist
	if err := os.MkdirAll(dbPath, 0755); err != nil {
		return nil, err
	}

	// Open LevelDB database
	db, err := leveldb.OpenFile(dbPath, nil)
	if err != nil {
		return nil, err
	}

	// Check for syncing key (0)
	//syncKey := []byte{0}
	_, err = db.Get(syncKey, nil)
	if err == leveldb.ErrNotFound {
		offsetBytes := make([]byte, 8)
		binary.BigEndian.PutUint64(offsetBytes, 0)
		if err := db.Put(syncKey, offsetBytes, nil); err != nil {
			db.Close()
			return nil, fmt.Errorf("failed to put key %v, value %v into db: %v", syncKey, offsetBytes, err)
		}
	} else if err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to get value for key %v in db: %v", syncKey, err)
	}

	return db, nil
}

func updateDB(datFile *os.File, db *leveldb.DB) error {
	common.RWMu.RLock()
	dbMu.Lock()
	defer common.RWMu.RUnlock()
	defer dbMu.Unlock()

	lastOffsetBytes, err := db.Get(syncKey, nil)
	if err != nil {
		return fmt.Errorf("failed to get syncKey: %v", err)
	}
	lastOffset := int64(binary.BigEndian.Uint64(lastOffsetBytes))

	datStat, err := datFile.Stat()
	if err != nil {
		return fmt.Errorf("failed to stat DAT file: %v", err)
	}

	if datStat.Size() < lastOffset {
		return fmt.Errorf("incorrect last offset; expected less than %v, got %v", datStat.Size(), lastOffset)
	}

	//common.PrintToLog(fmt.Sprintf("Updated DB: DAT size %v; lastoffset %v", datStat.Size(), lastOffset))

	maxBlockReads := 1000
	blockReadCount := 0

	for {
		// Limit number of blocks read
		if maxBlockReads == blockReadCount {
			break
		}

		if datStat.Size() == lastOffset {
			// Synced
			return nil
		}

		blockBytes, block, err := inout.ReadOneBlock(datFile, lastOffset)
		if err != nil {
			return fmt.Errorf("failed to read one block at offset %v: %v", lastOffset, err)
		}

		for _, hashArray := range block.BodyHashes {
			hash := hashArray[:]
			hashExist, err := db.Has(hash, nil)
			if err != nil {
				return fmt.Errorf("failed to check existence of hash %v: %v", hash, err)
			}

			if hashExist {
				// Get frequency
				freqBytes, err := db.Get(hash, nil)
				if err != nil {
					return fmt.Errorf("failed to get freq for hash %v: %v", hash, err)
				}

				// Increment by 1
				freq := binary.BigEndian.Uint32(freqBytes)
				freq++
				binary.BigEndian.PutUint32(freqBytes, freq)

				// Write frequency
				err = db.Put(hash, freqBytes, nil)
				if err != nil {
					return fmt.Errorf("failed to update frequency for hash %v to  %v in DB: %v", hash, freq, err)
				}
			} else {
				freqBytes := make([]byte, 4)
				binary.BigEndian.PutUint32(freqBytes, 1)

				err := db.Put(hash, freqBytes, nil)
				if err != nil {
					return fmt.Errorf("failed to insert new frequency for hash %v in DB: %v", hash, err)
				}
			}
		}

		// Update last offset in memory and db
		lastOffset += int64(len(blockBytes) + inout.MagicSize)
		binary.BigEndian.PutUint64(lastOffsetBytes, uint64(lastOffset))
		err = db.Put(syncKey, lastOffsetBytes, nil)
		if err != nil {
			return fmt.Errorf("failed to update last offset of %v: %v", lastOffset, err)
		}

		// Update number of blocks read
		blockReadCount += 1
	}

	return nil
}
