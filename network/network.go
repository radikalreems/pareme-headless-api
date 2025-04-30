package network

import (
	"context"
	"errors"
	"fmt"
	"net"
	"pareme/common"
	"sync"
	"time"
)

// Constants for peer connection status
const (
	StatusDisconnect     = 0                // Peer status: disconnected
	StatusConnecting     = 1                // Peer status: connecting
	StatusActive         = 2                // Peer status: active
	MaxPeers             = 100              // Maximum number of peers in AllPeers map
	ListenPort           = ":8080"          // Port for listening and default outbound connections
	DialTimeout          = 5 * time.Second  // Timeout for establishing peer connections
	ReadBufferSize       = 1024             // Buffer size for reading from peer connections
	requestPurgeInterval = 10 * time.Second // Interval for purging stale requests
)

var DialIPChan = make(chan string) // Channel to direct connect to an IP

// networkManager manages network connections and peer communications
// Returns: A channel for receiving IP addresses to connect to
func NetworkManager(ctx context.Context, wg *sync.WaitGroup) {
	common.PrintToLog("\nStarting up network manager...")

	// Channel for receiving IPs to dial
	//dialIPChan := make(chan string)

	// Start peer creation handler
	pendingPeerChan := peerMaker(ctx, wg) // Channel for new peer connections

	// Start main network loop
	wg.Add(1)
	go func() {
		defer wg.Done()

		for {
			select {
			case <-ctx.Done():
				// Shutdown signal received
				common.PrintToLog("Network shutting down")
				return

			case ip := <-DialIPChan:
				// Initiate connection to a new peer
				wg.Add(1)
				go connectToPeer(wg, ip, pendingPeerChan)

			case pendingPeer := <-pendingPeerChan:
				// Set up peer context with cancel capability
				peerCtx, cancel := context.WithCancel(ctx)
				pendingPeer.Context = peerCtx

				// Assign peer to AllPeers map
				var assignedIndex int
				assigned := false
				for i := range MaxPeers {
					if _, exists := common.AllPeers[i]; !exists {
						common.AllPeers[i] = &pendingPeer
						assignedIndex = i
						assigned = true
						break
					}
				}
				if assigned {
					common.PrintToLog(fmt.Sprintf("Added peer %s to AllPeers at index %d (total: %d)",
						pendingPeer.Address, assignedIndex, len(common.AllPeers)))
				} else {
					common.PrintToLog(fmt.Sprintf("Failed to add peer %s: max peers (%d) reached",
						pendingPeer.Address, MaxPeers))
					cancel() // Clean up peer context
					continue
				}

				// Handle the peer
				managePeer(ctx, peerCtx, cancel, wg, &pendingPeer)

			default:
				// Clean up disconnected peers
				for index, peer := range common.AllPeers {
					select {
					case <-peer.Context.Done():
						delete(common.AllPeers, index)
						common.PrintToLog(fmt.Sprintf("Removed peer %s from AllPeers (total: %d)",
							peer.Address, len(common.AllPeers)))
					default:
						continue
					}
				}
			}
		}
	}()
}

// peerMaker listens for incoming connections and creates new peers
// Returns: A chennel for pending peer connections
func peerMaker(ctx context.Context, wg *sync.WaitGroup) chan common.Peer {
	// Channels for connection handling
	acceptChan := make(chan common.Peer, 10)  // Channel for accepted peers
	pendingPeerChan := make(chan common.Peer) // Channel for pending peers

	common.PrintToLog(fmt.Sprintf("Network listening on %s", ListenPort))

	// Start main goroutine for listener management
	wg.Add(1)
	go func() {
		defer wg.Done()

		// Set up TCP listener
		listener, err := net.Listen("tcp", ListenPort)
		if err != nil {
			common.PrintToLog(fmt.Sprintf("Network error: %v", err))
			return
		}

		// Handle incoming connections concurrently
		wg.Add(1)
		go func() {
			defer wg.Done()

			for {
				conn, err := listener.Accept()
				if err != nil {
					if errors.Is(err, net.ErrClosed) {
						common.PrintToLog("Listener closed")
						return
					}
					common.PrintToLog(fmt.Sprintf("Accept error: %v", err))
					continue
				}

				// Parse remote address
				host, port, err := net.SplitHostPort(conn.RemoteAddr().String())
				if err != nil {
					common.PrintToLog("Error spliting address and port")
					return
				}
				peer := newPeer(host, port, conn, false)
				acceptChan <- peer
			}
		}()

		// Process accepted peers or handle shutdown
		for {
			select {
			case <-ctx.Done():
				common.PrintToLog("peerMaker Shutting Down")
				if err := listener.Close(); err != nil {
					common.PrintToLog(fmt.Sprintf("Failed to close listener: %v", err))
				}
				// Drain acceptChan to avoid blocking senders
				for len(acceptChan) > 0 {
					<-acceptChan
				}
				return

			case peer := <-acceptChan:
				response := RequestMessage(&peer, commandPing, nil)
				if response.Command != commandPing && response.Kind != messageKindResponse {
					common.PrintToLog(fmt.Sprintf("Potential peer %s failed ping request. dropping.", peer.Address))
					peer.Conn.Close()
				}
				common.PrintToLog(fmt.Sprintf("Connected to peer: %s", peer.Address))
				pendingPeerChan <- peer
			}
		}
	}()

	return pendingPeerChan
}

// connectToPeer establishes an outbound connection to a peer
func connectToPeer(wg *sync.WaitGroup, ip string, pendingPeerChan chan common.Peer) {
	defer wg.Done()

	_, _, err := net.SplitHostPort(ip)
	if err != nil {
		if net.ParseIP(ip) != nil {
			host := ip
			port := ListenPort[1:]
			ip = net.JoinHostPort(host, port)
		} else {
			common.PrintToLog(fmt.Sprintf("Invalid address %s: %v", ip, err))
			return
		}
	}

	conn, err := net.DialTimeout("tcp", ip, DialTimeout)
	if err != nil {
		common.PrintToLog(fmt.Sprintf("Failed to connect to %s: %v", ip, err))
		return
	}

	host, port, err := net.SplitHostPort(conn.RemoteAddr().String())
	if err != nil {
		common.PrintToLog(fmt.Sprintf("Failed to parse address %s: %v", conn.RemoteAddr().String(), err))
		conn.Close()
		return
	}
	peer := newPeer(host, port, conn, true)
	common.PrintToLog(fmt.Sprintf("Connected to peer: %s:%s", host, port))
	pendingPeerChan <- peer

	/*
		// Append default port if not specified
		_, _, err := net.SplitHostPort(ip)
		if err != nil {
			ip = ip + ListenPort
		}

		conn, err := net.DialTimeout("tcp", ip, DialTimeout)
		if err != nil {
			common.PrintToLog(fmt.Sprintf("Failed to connect to %s: %v", ip, err))
			return
		}

		// Create and register peer
		host, port, err := net.SplitHostPort(conn.RemoteAddr().String())
		if err != nil {
			common.PrintToLog(fmt.Sprintf("Failed to parse address %s: %v", conn.RemoteAddr().String(), err))
			conn.Close()
			return
		}
		peer := newPeer(host, port, conn, true)
		common.PrintToLog(fmt.Sprintf("Connected to peer: %s:%s", host, port))
		pendingPeerChan <- peer
	*/

}

// managePeer handles individual peer connections and communication
func managePeer(ctx context.Context, peerCtx context.Context, cancel context.CancelFunc, wg *sync.WaitGroup, peer *common.Peer) {
	var peerWg sync.WaitGroup
	var listenerWg sync.WaitGroup

	// Manages peer lifecycle and shutdown
	wg.Add(1)
	go func() {
		defer wg.Done()

		for {
			select {
			case <-peerCtx.Done():
				// Peer context canceled
				select {
				case <-ctx.Done():
					// Global shutdown
					common.PrintToLog(fmt.Sprintf("Closing connection for peer %s", peer.Address))
					peerWg.Wait()
					peer.Conn.Close()
					listenerWg.Wait()
					return
				default:
					// Peer connection closed
					common.PrintToLog(fmt.Sprintf("Connection closed for peer %v", peer.Address))
					peerWg.Wait()
					return
				}
			default:
				time.Sleep(1 * time.Second) // Prevent tight loop
			}
		}
	}()

	// Channel for coordinating message requests and responses
	RequestResponseChan := make(chan common.MessageRequest)

	peerWg.Add(2)
	go handleIncoming(peerCtx, cancel, &peerWg, &listenerWg, peer, RequestResponseChan)
	go handleOutgoing(peerCtx, &peerWg, peer, RequestResponseChan)
}

// peerListener reads incoming messages from a peer's connection
func peerListener(peerCtx context.Context, cancel context.CancelFunc, listenerWg *sync.WaitGroup, peer *common.Peer, readChan chan common.Message) {
	// Buffer for reading messages
	buff := make([]byte, ReadBufferSize) // Buffer for reading messages

	listenerWg.Add(1)
	go func() {
		defer listenerWg.Done()

		var leftover []byte // Accumulate partials messages
		for {
			n, err := peer.Conn.Read(buff)
			if err != nil {
				if !errors.Is(err, net.ErrClosed) {
					common.PrintToLog(fmt.Sprintf("Error reading from peer %s: %v", peer.Address, err))
				}
				select {
				case <-peerCtx.Done():
					// Context canceled; exit
					return
				default:
					// Peer disconnected; cancel context and exit
					cancel()
					return
				}
			}

			// Combine new data with leftover
			data := append(leftover, buff[:n]...)
			for len(data) >= messageHeaderSize { // Need at least header
				msgSize := int(data[0])<<8 | int(data[1]) // uint16 big-endian
				if len(data) < msgSize {
					break
				}

				// Extract full message
				msgBytes := data[:msgSize]
				message := common.Message{
					Size:        uint16(msgBytes[0])<<8 | uint16(msgBytes[1]),
					Kind:        msgBytes[2],
					Command:     msgBytes[3],
					Reference:   uint16(msgBytes[4])<<8 | uint16(msgBytes[5]),
					PayloadSize: uint16(msgBytes[6])<<8 | uint16(msgBytes[7]),
					Payload:     msgBytes[messageHeaderSize:msgSize],
				}

				readChan <- message   // Send message
				data = data[msgSize:] // Move to next message
			}
			leftover = data // Store partial
		}
	}()

}

// handleIncoming processes incoming messages for a peer
func handleIncoming(peerCtx context.Context, cancel context.CancelFunc, peerWg *sync.WaitGroup, listenerWg *sync.WaitGroup, peer *common.Peer, RequestResponseChan chan common.MessageRequest) {
	defer peerWg.Done()

	// Channel for incoming messages
	readChan := make(chan common.Message)

	// Start listener for peer messages
	peerListener(peerCtx, cancel, listenerWg, peer, readChan)

	// Track pending requests
	var pendingRequests []common.MessageRequest
	ticker := time.NewTicker(requestPurgeInterval)
	defer ticker.Stop()

	// Process incoming messages and requests
	for {
		select {
		case <-peerCtx.Done():
			// Shutdown: close pending request channels
			for _, req := range pendingRequests {
				close(req.ResponseChan)
			}
			common.PrintToLog(fmt.Sprintf("handleIncoming Closing for peer %s", peer.Address))
			return

		case msgReq := <-RequestResponseChan:
			// Add new requests to the pending list
			pendingRequests = append(pendingRequests, msgReq)

		case received := <-readChan:
			common.PrintToLog("Recieved: " + common.DescribeMessage(received))
			peer.LastSeen = time.Now()

			if received.Kind == messageKindResponse {
				// Handle response: match to pending request
				for i, req := range pendingRequests {
					if req.Message.Reference == received.Reference {
						select {
						case req.ResponseChan <- received:
							close(req.ResponseChan)
							// Remove from slice (swap and truncate)
							pendingRequests[i] = pendingRequests[len(pendingRequests)-1]
							pendingRequests = pendingRequests[:len(pendingRequests)-1]

						case <-req.ResponseChan:
							// Channel closed; remove request
							common.PrintToLog(fmt.Sprintf("Dropped response ref %d for closed request (remaining: %d)",
								received.Reference, len(pendingRequests)))
							pendingRequests[i] = pendingRequests[len(pendingRequests)-1]
							pendingRequests = pendingRequests[:len(pendingRequests)-1]
						}
						break
					}
				}
			} else {
				// Handle request: generate and send response
				response := respondToMessage(received, peer)
				sendResp := common.MessageRequest{
					Message:      response,
					ResponseChan: nil,
				}
				peer.SendChan <- sendResp
			}

		case <-ticker.C:
			// Purge closed request channels
			for i, req := range pendingRequests {
				select {
				case <-req.ResponseChan:
					// Channel closed, remove
					common.PrintToLog(fmt.Sprintf("Purged closed request ref %d (remaining: %d)",
						req.Message.Reference, len(pendingRequests)-1))
					pendingRequests[i] = pendingRequests[len(pendingRequests)-1]
					pendingRequests = pendingRequests[:len(pendingRequests)-1]
					i--
				default:
					// Channel open; keep request
				}
			}
		}
	}
}

// handleOutgoing sends messages to a peer
func handleOutgoing(peerCtx context.Context, peerWg *sync.WaitGroup, peer *common.Peer, RequestResponseChan chan common.MessageRequest) {
	defer peerWg.Done()

	for {
		select {
		case <-peerCtx.Done():
			common.PrintToLog(fmt.Sprintf("handleOutgoing Closing for peer %s", peer.Address))
			return

		case msgReq := <-peer.SendChan:
			// Forward requests with response channels
			if msgReq.ResponseChan != nil {
				RequestResponseChan <- msgReq
			}

			// Send message to peer
			_, err := peer.Conn.Write(serializeMessage(msgReq.Message))
			if err != nil {
				if errors.Is(err, net.ErrClosed) {
					common.PrintToLog(fmt.Sprintf("Connection closed for peer %s", peer.Address))
					if msgReq.ResponseChan != nil {
						close(msgReq.ResponseChan)
					}
					return
				}
				common.PrintToLog(fmt.Sprintf("Error writing message ref %d to peer %s: %v",
					msgReq.Message.Reference, peer.Address, err))
				if msgReq.ResponseChan != nil {
					close(msgReq.ResponseChan)
				}
				continue
			}
			/*
				if msgReq.ResponseChan != nil {
					RequestResponseChan <- msgReq
					// Send the message to the peer
					_, err := peer.Conn.Write(serializeMessage(msgReq.Message))
					if err != nil {
						if errors.Is(err, net.ErrClosed) {
							return
						}
						common.PrintToLog(fmt.Sprintf("Error writing message %d to peer", msgReq.Message.Reference))
						close(msgReq.ResponseChan)
						continue
					}
				} else {
					// Send the message to the peer
					_, err := peer.Conn.Write(serializeMessage(msgReq.Message))
					if err != nil {
						if errors.Is(err, net.ErrClosed) {
							return
						}
						common.PrintToLog(fmt.Sprintf("Error writing message %d to peer", msgReq.Message.Reference))
						continue
					}
				}
			*/
			common.PrintToLog("Sent: " + common.DescribeMessage(msgReq.Message))
			peer.LastSeen = time.Now()
		}
	}

}

// newPeer creates and initializes a new Peer instance
// Returns: An initialized Peer instance
func newPeer(address string, port string, conn net.Conn, isOutbound bool) common.Peer {
	peer := common.Peer{
		Address:    address,
		Port:       port,
		Conn:       conn,
		Context:    nil,
		Status:     StatusConnecting,
		IsOutbound: isOutbound,
		Version:    0,
		LastSeen:   time.Now(),
		SendChan:   make(chan common.MessageRequest, 10),
	}
	return peer
}

//--------------- QOL FUNCTIONS

// serializeMessage converts a Message to its byte representation
// Returns: The serialized byte slice
func serializeMessage(msg common.Message) []byte {
	totalLength := messageHeaderSize + len(msg.Payload)

	// Ensure Size and PayloadSize are consistent
	if int(msg.Size) != totalLength || msg.PayloadSize != uint16(len(msg.Payload)) {
		msg.Size = uint16(totalLength)
		msg.PayloadSize = uint16(len(msg.Payload))
	}

	// Allocate result buffer
	result := make([]byte, totalLength)

	// Serialize header (big-endian)
	result[0] = byte(msg.Size >> 8)
	result[1] = byte(msg.Size)
	result[2] = msg.Kind
	result[3] = msg.Command
	result[4] = byte(msg.Reference >> 8)
	result[5] = byte(msg.Reference)
	result[6] = byte(msg.PayloadSize >> 8)
	result[7] = byte(msg.PayloadSize)

	// Copy payload
	copy(result[messageHeaderSize:], msg.Payload)

	return result
}
