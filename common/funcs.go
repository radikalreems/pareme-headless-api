package common

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"math/big"
	"time"
)

var genesisBodyHash = [32]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}

// METHODS
func (b Block) ToByte() []byte {
	result := make([]byte, 0, BlockHeaderSize+32*b.BodySize)

	// Height: int (4 bytes)
	heightBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(heightBytes, uint32(b.Height))
	result = append(result, heightBytes...)

	// Timestamp: int 64 (8 bytes)
	timestampBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(timestampBytes, uint64(b.Timestamp))
	result = append(result, timestampBytes...)

	// PrevHash: [32]byte (32 bytes)
	result = append(result, b.PrevHash[:]...)

	// NBits: [4]byte (4 bytes)
	result = append(result, b.NBits[:]...)

	// Nonce: int (4 bytes)
	nonceBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(nonceBytes, uint32(b.Nonce))
	result = append(result, nonceBytes...)

	// BodySize: int (1 bytes)
	result = append(result, b.BodySize)

	// BodyHashes: [32]byte (32 bytes each)
	for _, hash := range b.BodyHashes {
		result = append(result, hash[:]...)
	}

	return result
}

func (b Block) Hash() [32]byte {
	buf := b.ToByte()
	return sha256.Sum256(buf[:])
}

// FUNCTIONS

// requestBlocks requests blocks from the writer by heights
// Returns: Blocks request by height
func RequestBlocks(heights []int) [][]Block {
	// Create channel to receive response
	responseChan := make(chan [][]Block)

	// Request heights from writer
	RequestChan <- ReadRequest{Heights: heights, Response: responseChan}

	// Return response after waiting
	return <-responseChan
}

// requestChainStats requests chain stats from the writer
// Returns: Latest height and total blocks
func RequestChainStats() (int, int) {
	// Create channel to receive response
	responseChan := make(chan [2]uint32)

	// Send request
	IndexRequestChan <- responseChan

	// Wait for response and return
	result := <-responseChan
	return int(result[0]), int(result[1])
}

func NewBlock(height int, prevHash [32]byte, nBits [4]byte, bodyHashes [][32]byte) (Block, error) {
	if len(bodyHashes) > MaxBodyHashes || len(bodyHashes) < 1 {
		return Block{}, fmt.Errorf("invalid amount of body hashes %v", len(bodyHashes))
	}
	block := Block{
		Height:     height,
		Timestamp:  time.Now().UnixMilli(),
		PrevHash:   prevHash,
		Nonce:      0,
		NBits:      nBits,
		BodySize:   uint8(len(bodyHashes)),
		BodyHashes: bodyHashes,
	}
	return block, nil
}

func GenesisBlock() Block {
	block := Block{
		Height:     1,
		Timestamp:  1230940800000,
		PrevHash:   [32]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		Nonce:      0,
		NBits:      MaxTargetNBits,
		BodySize:   1,
		BodyHashes: [][32]byte{genesisBodyHash},
	}
	return block
}

func ByteToBlock(data []byte) (Block, error) {
	var block Block

	// Height
	block.Height = int(binary.BigEndian.Uint32(data[:4]))

	// Timestamp
	block.Timestamp = int64(binary.BigEndian.Uint64(data[4:12]))

	// PrevHash
	copy(block.PrevHash[:], data[12:44])

	// NBits
	copy(block.NBits[:], data[44:48])

	// Nonce
	block.Nonce = int(binary.BigEndian.Uint32(data[48:52]))

	// BodySize
	if (len(data)-BlockHeaderSize)%32 != 0 {
		return Block{}, fmt.Errorf("invalid data size")
	}
	block.BodySize = uint8((len(data) - BlockHeaderSize) / 32)
	if block.BodySize > MaxBodyHashes || block.BodySize < 1 {
		return Block{}, fmt.Errorf("invalid hash amount of %v", block.BodySize)
	}

	// BodyHashes
	var hash [32]byte

	for i := range int(block.BodySize) {
		copy(hash[:], data[BlockHeaderSize+i*32:(BlockHeaderSize+32)+i*32])
		block.BodyHashes = append(block.BodyHashes, hash)
	}

	return block, nil
}

/*
func BlockToByte(b Block) [BlockSize]byte {
	var result [BlockSize]byte

	// Height: int (4 bytes)
	binary.BigEndian.PutUint32(result[0:4], uint32(b.Height))

	// Timestamp: int 64 (8 bytes)
	binary.BigEndian.PutUint64(result[4:12], uint64(b.Timestamp))

	// PrevHash: [32]byte (32 bytes)
	copy(result[12:44], b.PrevHash[:])

	// NBits: [4]byte (4 bytes)
	copy(result[44:48], b.NBits[:])

	// Nonce: int (4 bytes)
	binary.BigEndian.PutUint32(result[48:52], uint32(b.Nonce))

	// BodyHash: [32]byte (32 bytes)
	copy(result[52:84], b.BodyHash[:])

	return result
}
*/

func NBitsToTarget(nBits [4]byte) [32]byte {
	size := int(nBits[0])
	mantissa := uint32(nBits[1])<<16 | uint32(nBits[2])<<8 | uint32(nBits[3])

	target := new(big.Int).SetUint64(uint64(mantissa))
	if size > 3 {
		target.Lsh(target, uint(8*(size-3)))
	} else if size < 3 {
		target.Rsh(target, uint(8*(3-size)))
	}

	bytes := target.Bytes()
	result := [32]byte{}
	if len(bytes) > 32 {
		copy(result[:], bytes[len(bytes)-32:])
	} else {
		copy(result[32-len(bytes):], bytes)
	}

	return result
}

func TargetToNBits(target [32]byte) [4]byte {
	t := new(big.Int).SetBytes(target[:])
	bytes := t.Bytes()
	size := min(len(bytes), 255)

	var mantissa uint32
	if size == 0 {
		return [4]byte{}
	} else if size <= 3 {
		mantissa = uint32(t.Uint64()) << (8 * (3 - size))
	} else {
		shifted := new(big.Int).Rsh(t, uint(8*(size-3)))
		mantissa = uint32(shifted.Uint64())
	}

	return [4]byte{
		byte(size),
		byte(mantissa >> 16),
		byte(mantissa >> 8),
		byte(mantissa),
	}
}
