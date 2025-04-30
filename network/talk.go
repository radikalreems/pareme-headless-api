package network

import (
	"encoding/binary"
	"fmt"
	"net"
	"pareme/common"
	"time"
)

const (
	messageHeaderSize     = 8   // Size of the Message struct excluding the payload (in bytes)
	messageKindRequest    = 0   // Message kind for requests
	messageKindResponse   = 1   // Message kind for responses
	commandPing           = 0   // Command for ping request/response
	commandLatestHeight   = 1   // Command for requesting/returning chain height
	commandBlockRequest   = 2   // Command for requesting/returning blocks
	commandBlockBroadcast = 3   // Command for broadcasting a block
	commandPeerDiscovery  = 4   // Command for sharing peers
	syncChunkSize         = 100 // Number of blocks to request per sync chunk
)

// nextReferenceID tracks the next available reference ID for request messages
var nextReferenceID uint16 = 0

// syncToPeers synchronizes the local blockchain with peers by fetching missing blocks
// Returns an error if synchronization fails, or nil if successful, already synced, or no peers are available
func SyncToPeers() error {
	if len(common.AllPeers) == 0 {
		return nil
	}

	// Fetch local latest height
	latestHeight, _ := common.RequestChainStats()

	// Select a peer (random pick)
	var selectedPeer *common.Peer          // Which peer to ask
	for _, peer := range common.AllPeers { // Pick random peer
		selectedPeer = peer
		break
	}

	// Request peer's latest height
	heightResponse := RequestMessage(selectedPeer, commandLatestHeight, nil)
	peerLatestHeight := int(binary.BigEndian.Uint32(heightResponse.Payload))
	heightDifference := peerLatestHeight - latestHeight

	// Check if sync is needed
	if heightDifference < 10 { // Allow small differences
		common.PrintToLog("Already synced with peer!")
		return nil
	}

	common.PrintToLog(fmt.Sprintf("Chain is out of sync with peer: latest: %d, peers latest: %d", latestHeight, peerLatestHeight))

	// Calculate number of chunks to fetch
	chunkCount := (heightDifference / syncChunkSize) + 1

	for i := range chunkCount {
		// Determine height range for this chunk
		startHeight := latestHeight + 1 + i*syncChunkSize
		endHeight := latestHeight + (i+1)*syncChunkSize
		if i == chunkCount-1 {
			endHeight = peerLatestHeight
		}

		// Build list of heights to request
		var heights []int
		for j := startHeight; j < endHeight+1; j++ {
			heights = append(heights, j)
		}

		// Encode heights into payload (4 bytes per height)
		heightsPayload := make([]byte, len(heights)*4)
		for j, value := range heights {
			binary.BigEndian.PutUint32(heightsPayload[j*4:j*4+4], uint32(value))
		}

		// Select a peer for block request (random pick)
		var blockPeer *common.Peer             // Which peer to ask
		for _, peer := range common.AllPeers { // Pick random peer
			blockPeer = peer
			break
		}

		// Request blocks for the specified heights
		blocksResponse := RequestMessage(blockPeer, commandBlockRequest, heightsPayload)

		// Validate response payload size
		if int(blocksResponse.PayloadSize) != len(blocksResponse.Payload) {
			return fmt.Errorf("response payload size %d does not match actual size %d",
				blocksResponse.PayloadSize, len(blocksResponse.Payload))
		}

		blocks := make([]common.Block, 0)
		buf := make([]byte, 0, blocksResponse.PayloadSize)
		copy(buf, blocksResponse.Payload)
		for len(buf) > 0 {
			bodyHashesSize := uint8((buf[common.BlockHeaderSize-1 : common.BlockHeaderSize])[0])
			blockBytes := buf[:common.BlockHeaderSize+bodyHashesSize]
			block, err := common.ByteToBlock(blockBytes)
			if err != nil {
				return fmt.Errorf("failed to convert bytes to block: %v", err)
			}
			blocks = append(blocks, block)

			buf = buf[common.BlockHeaderSize+bodyHashesSize:]
		}

		/*
			// Split payload into individual block byte arrays
			numOfBlocks := int(blocksResponse.PayloadSize / common.BlockSize)
			BlockBytes := make([][common.BlockSize]byte, numOfBlocks)
			for j := range numOfBlocks {
				copy(BlockBytes[j][:], blocksResponse.Payload[j*common.BlockSize:(j+1)*common.BlockSize])
			}

			// Convert block bytes to Block structs
			var blocks []common.Block
			for _, blockByte := range BlockBytes {
				blocks = append(blocks, common.ByteToBlock(blockByte))
			}
		*/

		// Prepare and send write block request
		writeReq := common.WriteBlockRequest{
			Blocks: blocks,
			Type:   "sync",
			From:   blockPeer,
		}

		common.WriteBlockChan <- writeReq

	}

	return nil

}

func FindPeers() {

	if len(common.AllPeers) == 0 {
		DialIPChan <- "170.75.161.198"
		time.Sleep(2 * time.Second)
		return
	}

	// Filter peers by outbound
	var outbound []*common.Peer
	for _, peer := range common.AllPeers {
		if peer.IsOutbound {
			outbound = append(outbound, peer)
		}
	}

	// If not enough outbound, find more
	if len(outbound) < 10 {
		// Send peer discovery requests to current peers
		requestingAmt := 10 - len(outbound)

		var payload []byte
		payload = append(payload, byte(requestingAmt))
		response := RequestMessage(outbound[0], commandPeerDiscovery, payload)

		amtGiven := response.PayloadSize / 16

		if amtGiven == 0 {
			return
		}

		ips := make([]net.IP, 0, amtGiven)

		for i := range amtGiven {
			ip := net.IP(response.Payload[i*16 : (i*16)+16])
			ips = append(ips, ip)
		}

		for _, ip := range ips {
			DialIPChan <- ip.String()
		}
	}

}

// requestMessage sends a message to a peer and waits for its response
// Returns: The response Message from the peer
func RequestMessage(peer *common.Peer, command uint8, payload []byte) common.Message {
	// Create a new request message
	msg := newMessage(messageKindRequest, command, nextReferenceID, payload)
	nextReferenceID++

	// Set up a channel for the response
	responseChan := make(chan common.Message) // Unbuffered for synchronous response
	msgReq := common.MessageRequest{
		Message:      msg,
		ResponseChan: responseChan,
	}

	// Send the request to the peer
	peer.SendChan <- msgReq

	// Wait for and return the response
	return <-responseChan
}

// broadcastBlock sends a block to all peers except those in the exclusion list
func BroadcastBlock(block common.Block, excludedPeers []*common.Peer) {
	// Convert block to a fixed-size byte array
	blockBytes := block.ToByte()

	// Convert to slice for message payload
	blockPayload := blockBytes[:]

	// Broadcase to each non-excluded peer
	for _, peer := range common.AllPeers { // Send to all peers except exclusion
		isExcluded := false
		for i := range excludedPeers {
			if excludedPeers[i] == nil {
				continue
			}
			if excludedPeers[i].Address == peer.Address {
				isExcluded = true
				continue
			}
		}
		if isExcluded {
			continue
		}

		// Send block to peer and log response
		common.PrintToLog(fmt.Sprintf("Broadcasting Block %d to peer....", block.Height))
		response := RequestMessage(peer, commandBlockBroadcast, blockPayload)
		common.PrintToLog(common.DescribeMessage(response))
	}
}

// newMessage creates a new Message with the specified parameters
func newMessage(kind uint8, command uint8, reference uint16, payload []byte) common.Message {
	payloadSize := uint16(len(payload))
	totalSize := uint16(messageHeaderSize + len(payload)) // Header (8 bytes) + payload

	return common.Message{
		Size:        totalSize,
		Kind:        kind,
		Command:     command,
		Reference:   reference,
		PayloadSize: payloadSize,
		Payload:     payload,
	}
}

// respondToMessage processes an incoming message and returns an appropriate response
// Returns: A response Message, or an empty Message for invalid/unknown commands
func respondToMessage(request common.Message, from *common.Peer) common.Message {
	switch request.Command {
	case commandPing:
		// Handle ping request: return a simple pong response with no payload
		return newMessage(messageKindResponse, commandPing, request.Reference, nil)

	case commandLatestHeight:
		// Handle height request: return current chain height as a 4-byte uint32
		height, _ := common.RequestChainStats() // Ignore totalBlocks
		buf := make([]byte, 4)
		binary.BigEndian.PutUint32(buf, uint32(height))
		return newMessage(messageKindResponse, commandLatestHeight, request.Reference, buf)

	case commandBlockRequest:
		// Validate payload size
		if int(request.PayloadSize) != len(request.Payload) {
			common.PrintToLog(fmt.Sprintf("Invalid payload size for ref %d: expected %v, got %v",
				request.Reference, request.PayloadSize, len(request.Payload)))
			return common.Message{}
		}
		if request.PayloadSize == 0 {
			common.PrintToLog(fmt.Sprintf("Empty payload for block request ref %d", request.Reference))
			return common.Message{}
		}

		// Extract heights from payload (4 bytes per height)
		numHeights := int(request.PayloadSize / 4)
		heights := make([]int, numHeights)
		for i := range numHeights {
			value := binary.BigEndian.Uint32(request.Payload[i*4 : i*4+4])
			heights[i] = int(int32(value))
		}

		// Fetch requested blocks
		blockGroups := common.RequestBlocks(heights)
		var blocks []common.Block
		for _, group := range blockGroups {
			blocks = append(blocks, group...)
		}

		// Seralize blocks into payload
		payload := make([]byte, 0)
		for _, block := range blocks {
			blockByte := block.ToByte()
			payload = append(payload, blockByte[:]...)
		}
		// Return response with block data
		return newMessage(messageKindResponse, commandBlockRequest, request.Reference, payload)

	case commandBlockBroadcast:
		// Validate payload size
		if int(request.PayloadSize) != len(request.Payload) {
			common.PrintToLog(fmt.Sprintf("Invalid payload size for ref %d: expected %d, got %d",
				request.Reference, request.PayloadSize, len(request.Payload)))
			return common.Message{}
		}
		if request.PayloadSize == 0 {
			common.PrintToLog(fmt.Sprintf("Empty payload for block broadcase ref %d", request.Reference))
			return common.Message{}
		}

		// Extract block from payload
		block, err := common.ByteToBlock([]byte(request.Payload))
		if err != nil {
			common.PrintToLog(fmt.Sprintf("Failed to convert bytes into a block: %v", err))
			return common.Message{}
		}

		// Check for duplicate block
		currentHeight, _ := common.RequestChainStats()
		if currentHeight >= block.Height {
			existingBlocks := common.RequestBlocks([]int{block.Height})[0]
			for _, existing := range existingBlocks {
				if existing.Hash() == block.Hash() {
					// Duplicate block
					// Return a pong Message to send confirmation
					common.PrintToLog(fmt.Sprintf("Duplicate block %d detected; dropping", block.Height))
					return newMessage(messageKindResponse, commandPing, request.Reference, nil)
				}
			}
		}

		// Send block to writer for processing
		common.PrintToLog(fmt.Sprintf("Received new block %d; sending to writer", block.Height))
		writeReq := common.WriteBlockRequest{
			Blocks: []common.Block{block},
			Type:   "received",
			From:   from,
		}
		common.WriteBlockChan <- writeReq

		// Confirm receipt with a pong response
		return newMessage(messageKindResponse, commandPing, request.Reference, nil)

	case commandPeerDiscovery:
		// Determine how many peers are being requested
		var numOfReqPeers uint8
		numOfReqPeers = (request.Payload)[0]

		// Gather as many peers as requested
		var peersToSend []string
		for i := range common.AllPeers {
			if common.AllPeers[i].Address == from.Address {
				continue
			}

			peersToSend = append(peersToSend, common.AllPeers[i].Address)

			if len(peersToSend) >= int(numOfReqPeers) {
				break
			}
		}

		var payload []byte
		for _, address := range peersToSend {
			// IP (can be IPv4 or IPv6)
			ip := net.ParseIP(address)

			// Ensure it's in 16-byte form
			ipBytes := ip.To16()

			// Convert to payload
			payload = append(payload, []byte(ipBytes)...)
		}

		return newMessage(messageKindResponse, commandPeerDiscovery, request.Reference, payload)

	default:
		// Handle unknown command
		common.PrintToLog(fmt.Sprintf("Unknown command %d for ref %d", request.Command, request.Reference))
		return common.Message{}
	}
}
