package common

import (
	"context"
	"net"
	"sync"
	"time"
)

const (
	//BlockSize = 84 // Size of a block (not including 4 byte magic when written)
	MaxNonce        = 1_000_000_000
	BlockHeaderSize = 53               // Height + Timestamp + PrevHash + NBits + Nonce + BodySize
	MaxBodyHashes   = 10               // Max number of Body Hashes in a block
	BlocksDir       = "blocks"         // Directory for block data
	DirIndexFile    = "dir0000.idx"    // Directory index file
	OffIndexFile    = "off0000.idx"    // Offset index file
	DataFile        = "pareme0000.dat" // Block data file
)

var (
	WriteBlockChan   = make(chan WriteBlockRequest, 100) // Send a Block to have it verified and written to file
	RequestChan      = make(chan ReadRequest)            // Send a readRequest to retrieve a specific block
	IndexRequestChan = make(chan chan [2]uint32, 1)      // Send a channel to receive index info in it
)

var (
	MaxTarget = [32]byte{0x00, 0x00, 0xFF, 0xFF, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}
	MaxTargetNBits = TargetToNBits(MaxTarget)
)

var RWMu sync.RWMutex

var AllPeers map[int]*Peer = make(map[int]*Peer)

// Block structure defining the parts of a block
type Block struct { // HEADER: 53 BYTES | IN FILE: 4 MAGIC + 53 + (32 * Number of body hashes)
	Height     int        // 4 bytes
	Timestamp  int64      // 8 bytes
	PrevHash   [32]byte   // 32 bytes
	NBits      [4]byte    // 4 bytes
	Nonce      int        // 4 bytes
	BodySize   uint8      // 1 byte
	BodyHashes [][32]byte // 32 bytes each ( 10 max )
}

// Peer represents a network peer with its connection details and status
type Peer struct {
	Address    string              // Network address of the peer
	Port       string              // Port number for the peer
	Conn       net.Conn            // Active network connection to the peer
	Context    context.Context     // Context for managing peer lifecycle
	Status     int                 // Connection status (e.g., StatusConnecting, StatusActive)
	IsOutbound bool                // True if this is an outbound connection
	Version    int                 // Protocol version of the peer
	LastSeen   time.Time           // Last time the peer was active
	SendChan   chan MessageRequest // Channel for sending message requests
}

// Message represents the structure of a blockchain network message
type Message struct {
	Size        uint16 // Total message length in bytes (2 bytes)
	Kind        uint8  // Message type: 0 for request, 1 for Response (1 byte)
	Command     uint8  // Specific action (e.g., for requests: 1=latest height, 2=block request) (1 byte)
	Reference   uint16 // Unique ID set by requester, echoed in response (2 bytes)
	PayloadSize uint16 // Length of the payload data in bytes (2 bytes)
	Payload     []byte // Variable-length data (e.g., heights, blocks)
}

// MessageRequest pairs a message with a channel to reveive its response
type MessageRequest struct {
	Message      Message      // The request message
	ResponseChan chan Message // Channel to receive the corresponding response
}

// ReadRequest represents a request to read a block by height
type ReadRequest struct {
	Heights  []int
	Response chan [][]Block
}

// WriteBlockRequest represents a request to to write blocks, including its reason
type WriteBlockRequest struct {
	Blocks []Block // Blocks to be written
	Type   string  // "sync" | "mined" | "received"
	From   *Peer   // If received, from where
}

// MiningState tracks the different attributes of the miner
var MiningState struct {
	Active    bool // Miner is mining or preparing to mine
	IsFinding bool // Miner is actively mining a block
	Height    int  // Current height the miner is at
}
