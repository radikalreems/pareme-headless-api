package main

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"pareme/common"
	"pareme/explorer"
	"pareme/inout"
	"pareme/network"
	"strings"
	"sync"
)

// Initializes and runs the Pareme blockchain node
func main() {
	fmt.Println("Starting Pareme...")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel() // Ensure canellation on exit
	var wg sync.WaitGroup

	// Initialize the printer for logging to .log file
	common.InitPrinter(ctx, &wg)

	// Start the networking goroutine
	network.NetworkManager(ctx, &wg)

	// Sync chain data from own files
	err := inout.InitFiles()
	if err != nil {
		common.PrintToLog(fmt.Sprintf("Sync failed: %v", err))
		cancel()
		wg.Wait()
		return
	}

	// Chain is valid; start the block writer goroutine
	_, err = inout.BlockWriter(ctx, &wg)
	if err != nil {
		common.PrintToLog(fmt.Sprintf("Blockwriter failed: %v", err))
		cancel()
		wg.Wait()
		return
	}

	/*
		network.FindPeers()

		// Sync chain data from peers
		err = network.SyncToPeers()
		if err != nil {
			common.PrintToLog(fmt.Sprintf("Syncing chain from peers failed: %v", err))
			cancel()
			wg.Wait()
			return
		}
	*/

	// Start the Stats manager
	err = explorer.StatsManager(ctx, &wg)
	if err != nil {
		common.PrintToLog("Failed to start Stats Manager")
	}

	fmt.Println("Program running. Type 'exit' to quit.")

	reader := bufio.NewReader(os.Stdin)
	for {
		select {
		case <-ctx.Done():
			fmt.Println("Context canceled, exiting...")
			wg.Wait()
			return
		default:
			fmt.Print("Pareme> ")
			input, err := reader.ReadString('\n')
			if err != nil {
				common.PrintToLog(fmt.Sprintf("Input error: %v", err))
				break
			}

			input = strings.TrimSpace(input)

			switch {
			case input == "exit":
				cancel()
				wg.Wait()
				return
			default:
				fmt.Println("Unknown command")
			}
		}
	}

}
