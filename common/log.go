package common

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// printChan buffers log messages to be written to the log file
var printChan chan string

const (
	logDir  = "common"
	logFile = "pareme.log"
)

// initPrinter starts a goroutine to handle logging to pareme.log
func InitPrinter(ctx context.Context, wg *sync.WaitGroup) {
	PrintToLog("\nStarting up printer...")
	logPath := filepath.Join(logDir, logFile)

	// Truncate existing log file if it exists
	if _, err := os.Stat(logPath); err == nil {
		if err := os.Truncate(logPath, 0); err != nil {
			panic("Failed to truncate pareme.log" + err.Error())
		}
	}

	// Open log file in append mode
	f, err := os.OpenFile(logPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		panic("failed to open pareme.log: " + err.Error())
	}

	// Initialize print channel with buffer
	printChan = make(chan string, 100)

	wg.Add(1)
	go func() {
		defer wg.Done()
		defer f.Close()
		defer close(printChan)

		for {
			select {
			case msg := <-printChan:
				// Write log message to file
				if _, err := f.WriteString(msg + "\n"); err != nil {
					continue
				}
			case <-ctx.Done():
				// Drain remaining messages before shutdown
				time.Sleep(2 * time.Second) // Allow time for new messages
				for len(printChan) > 0 {
					if msg, ok := <-printChan; ok {
						f.WriteString(msg + "\n")
					}
				}
				f.WriteString("Printer shutting down")
				return
			}
		}
	}()
}

// printToLog sends a message to the log file via the print channel
func PrintToLog(s string) {
	select {
	case printChan <- s: // Send message to printer
	case <-time.After(1 * time.Second): // Drop message if channel is full after 1s
	}
}

func DescribeMessage(msg Message) string {
	var kindStr, cmdStr string

	// Kind: Request or Response
	switch msg.Kind {
	case 0:
		kindStr = "Request"
	case 1:
		kindStr = "Response"
	default:
		kindStr = "Unknown Kind"
	}

	// Command: Specific action
	switch msg.Command {
	case 0:
		if msg.Kind == 0 {
			cmdStr = "Ping"
		} else {
			cmdStr = "Pong"
		}
	case 1:
		cmdStr = "Latest Height"
	case 2:
		cmdStr = "Block Request"
	case 3:
		cmdStr = "Block Broadcast"
	default:
		cmdStr = "Unknown Command"
	}

	// Combine into a readable string with Reference and PayloadSize
	return fmt.Sprintf("%s for %s (Reference: %d, Payload Size: %d bytes)",
		kindStr, cmdStr, msg.Reference, msg.PayloadSize)
}

func DescribeMessageFrontEnd(msg Message) string {
	var kindStr, cmdStr, result string

	// Kind: Request or Response
	switch msg.Kind {
	case 0:
		kindStr = "Sent Request"
	case 1:
		kindStr = "Received Response"
	default:
		kindStr = "Unknown Kind"
	}

	// Command: Specific action
	switch msg.Command {
	case 0:
		if msg.Kind == 0 {
			cmdStr = "Ping"
		} else {
			cmdStr = "Pong"
		}
		result = fmt.Sprintf("%s for %s", kindStr, cmdStr)
	case 1:
		cmdStr = "Latest Height"
		latestHeight := binary.BigEndian.Uint32(msg.Payload)
		result = fmt.Sprintf("%s for %s. | Latest Height: %d", kindStr, cmdStr, latestHeight)
	case 2:
		cmdStr = "Block Request"
		result = fmt.Sprintf("%s for %s", kindStr, cmdStr)
	case 3:
		cmdStr = "Block Broadcast"
		result = fmt.Sprintf("%s for %s", kindStr, cmdStr)
	default:
		cmdStr = "Unknown Command"
	}

	// Combine into a readable string with Reference and PayloadSize
	return result
}
