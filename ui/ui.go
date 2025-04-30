package ui

import (
	"context"
	"fmt"
	"pareme/common"
	"pareme/network"
	"sync"
	"time"

	"slices"

	"fyne.io/fyne/v2"
	"fyne.io/fyne/v2/app"
	"fyne.io/fyne/v2/canvas"
	"fyne.io/fyne/v2/container"
	"fyne.io/fyne/v2/layout"
	"fyne.io/fyne/v2/theme"
	"fyne.io/fyne/v2/widget"
)

// peerToggle defines the structure for a peer selection checkbox in the UI
type peerToggle struct {
	toggle *widget.Check
	id     int
	peer   *common.Peer
}

// Global outputText displays log messages in the UI
var outputText *widget.Label

// runUI initializes and runs the main UI window with all components
func RunUI(ctx context.Context, cancel context.CancelFunc, wg *sync.WaitGroup, consoleMineChan chan []string) {
	// Create Fyne UI
	a := app.New()
	w := a.NewWindow("Pareme Blockchain Node")
	w.Resize(fyne.NewSize(600, 400))

	// Create Output Text field for logging
	outputText = widget.NewLabel("Pareme Node Started\n")
	outputText.Wrapping = fyne.TextWrapWord

	var peerToggles = make(map[int]*peerToggle)
	var peerTogglesMutex sync.Mutex
	networkGroup := createNetworkGroup(cancel)
	miningGroup := createMiningGroup(consoleMineChan)

	peerControls, peerListContainer, pingButton, heightButton := createPeerControls(peerToggles, &peerTogglesMutex)
	networkGroup.Add(peerControls)

	// Start peer list updater
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				updatePeerList(peerToggles, &peerTogglesMutex, peerListContainer, pingButton, heightButton)
			}
		}
	}()

	// Button to stop node and quit
	stopButton := widget.NewButton("Stop Node", func() {
		common.PrintToLog("Recieved 'stop' command")
		cancel()
		wg.Wait()
		outputText.SetText(outputText.Text + "Node stopped\n")
		a.Quit()
	})
	stopButton.Importance = widget.WarningImportance

	// --------UI OVERLAY--------
	outputScroll := container.NewScroll(outputText)
	outputScroll.SetMinSize(fyne.NewSize(0, 200))

	// Main title
	mainTitle := createTitleLabel("Pareme Blockchain Node")
	mainTitle.TextSize = 24

	// UI overlay
	content := container.NewVBox(
		container.NewPadded(mainTitle),
		networkGroup,
		newSpacer(),
		miningGroup,
		newSpacer(),
		stopButton,
		outputScroll,
	)

	// Run UI
	w.SetContent(content)
	updatePeerList(peerToggles, &peerTogglesMutex, peerListContainer, pingButton, heightButton)
	w.ShowAndRun()
}

// --------- Groups
// createNetworkGroup builds the network control section
func createNetworkGroup(cancel context.CancelFunc) *fyne.Container {
	// Direct IP connect field
	directIPEntry := widget.NewEntry()
	directIPEntry.SetPlaceHolder("Enter IP address")
	directConnectButton := widget.NewButton("Direct Connect", func() {
		if ip := directIPEntry.Text; ip != "" {
			common.PrintToLog(fmt.Sprintf("Connecting to IP: %s", ip))
			network.DialIPChan <- ip
			outputText.SetText(outputText.Text + fmt.Sprintf("Connecting to %s\n", ip))
		}
	})
	directConnectRow := container.NewBorder(nil, nil, nil, directConnectButton, directIPEntry)

	// Connect to the existing network
	networkConnectButton := widget.NewButton("Connect to Network", func() {
		network.FindPeers()

		// Sync chain data from peers
		err := network.SyncToPeers()
		if err != nil {
			common.PrintToLog(fmt.Sprintf("Syncing chain from peers failed: %v", err))
			cancel()
			return
		}
	})

	// Create the baseline network group container
	return container.NewVBox(
		createTitleLabel("Network Controls"),
		directConnectRow,
		container.NewPadded(networkConnectButton),
		//container.NewPadded(ipEntry),
		//container.NewPadded(connectButton),
		layout.NewSpacer(),
	)
}

// createPeerControls constructs the peer interaction section
func createPeerControls(peerToggles map[int]*peerToggle, mutex *sync.Mutex) (*fyne.Container, *fyne.Container, *widget.Button, *widget.Button) {
	peerListContainer := container.NewVBox()

	// Ping button to send a ping to selected peer
	pingButton := widget.NewButton("Ping", func() {
		common.PrintToLog("Received 'ping' command")
		mutex.Lock()
		defer mutex.Unlock()

		selected := false
		for _, pt := range peerToggles {
			if pt.toggle.Checked {
				response := network.RequestMessage(pt.peer, 0, nil)
				outputText.SetText(outputText.Text + common.DescribeMessageFrontEnd(response) + "\n")
				selected = true
			}
		}

		if !selected {
			outputText.SetText(outputText.Text + "No peers selected for ping\n")
		}

	})
	pingButton.Disable()

	// Height button to request latest height from selected peer
	heightButton := widget.NewButton("Get Latest Height", func() {
		common.PrintToLog("Received 'height' command")
		mutex.Lock()
		defer mutex.Unlock()

		selected := false
		for _, pt := range peerToggles {
			if pt.toggle.Checked {
				response := network.RequestMessage(pt.peer, 1, nil)
				outputText.SetText(outputText.Text + common.DescribeMessageFrontEnd(response) + "\n")
				selected = true
			}
		}
		if !selected {
			outputText.SetText(outputText.Text + "No peers selected for height\n")
		}
	})
	heightButton.Disable()

	peerControls := container.NewVBox(
		createTitleLabel("Connected Peers"),
		container.NewPadded(container.NewVScroll(peerListContainer)),
		container.NewHBox(
			container.NewPadded(pingButton),
			container.NewPadded(heightButton),
		),
		layout.NewSpacer(),
	)

	return peerControls, peerListContainer, pingButton, heightButton

}

// createMiningGroup sets up the mining control section
func createMiningGroup(consoleMineChan chan []string) *fyne.Container {

	hashEntries := []*widget.Entry{}
	hashContainer := container.NewVBox()

	createEntryRow := func(entry *widget.Entry, canRemove bool) *fyne.Container {
		rowContainer := container.NewBorder(nil, nil, nil, nil, entry)
		if !canRemove {
			return rowContainer
		}

		removeButton := widget.NewButton("Remove", func() {
			// Find index and remove
			for i, e := range hashEntries {
				if e == entry {
					hashEntries = slices.Delete(hashEntries, i, i+1)
					break
				}
			}
			hashContainer.Remove(rowContainer)
			hashContainer.Refresh()
		})
		rowContainer = container.NewBorder(nil, nil, nil, removeButton, entry)
		return rowContainer
	}

	firstEntry := widget.NewEntry()
	firstEntry.SetPlaceHolder("Enter Hash To Mine")
	firstEntry.Text = "78d031901879f64da61a41dd1e0c8a16d1ace1b9b61133db079413a816e765d2" // "pareme"

	hashEntries = append(hashEntries, firstEntry)
	hashContainer.Add(createEntryRow(firstEntry, false))

	addButton := widget.NewButton("Add Hash", func() {
		if len(hashEntries) < 10 {
			newEntry := widget.NewEntry()
			newEntry.SetPlaceHolder("Enter Hash To Mine")

			hashEntries = append(hashEntries, newEntry)
			hashContainer.Add(createEntryRow(newEntry, true))
			hashContainer.Refresh()
		}
	})

	//hashEntry := widget.NewEntry()
	//hashEntry.SetPlaceHolder("Enter Hash To Mine")
	//hashEntry.Text = "78d031901879f64da61a41dd1e0c8a16d1ace1b9b61133db079413a816e765d2" // "pareme"

	// Button to start mining with given hash
	startMineButton := widget.NewButton("Start Mining", func() {
		common.PrintToLog("Recieved 'start mine' command")
		var collected []string
		for _, entry := range hashEntries {
			if entry.Text == "" {
				continue
			}
			collected = append(collected, entry.Text)
		}
		common.PrintToLog(fmt.Sprintf("hashes to mine: %v", collected))
		consoleMineChan <- collected
		outputText.SetText(outputText.Text + "Mining started\n")
	})
	startMineButton.Importance = widget.HighImportance

	// Button to stop mining
	stopMineButton := widget.NewButton("Stop Mining", func() {
		common.PrintToLog("Recieved 'stop mine' command")
		consoleMineChan <- []string{}
		outputText.SetText(outputText.Text + "Mining stopped\n")
	})

	return container.NewVBox(
		createTitleLabel("Mining Controls"),
		hashContainer,
		addButton,
		//container.NewPadded(hashEntry),
		container.NewHBox(
			container.NewPadded(startMineButton),
			container.NewPadded(stopMineButton),
		),
		layout.NewSpacer(),
	)
}

// updatePeerList synchronizes the UI peer list with the current set of connected peers
func updatePeerList(peerToggles map[int]*peerToggle, mutex *sync.Mutex, container *fyne.Container, pingButton *widget.Button, heightButton *widget.Button) {
	mutex.Lock()
	defer mutex.Unlock()

	currentPeers := make(map[int]bool)
	for id := range common.AllPeers {
		currentPeers[id] = true

		_, exists := peerToggles[id]
		if !exists {
			peer := common.AllPeers[id]
			toggle := widget.NewCheck(fmt.Sprintf("Peer %d: %v", id, peer.Address), nil)
			peerToggles[id] = &peerToggle{
				toggle: toggle,
				id:     id,
				peer:   peer,
			}
			container.Add(toggle)
		}
	}
	// Remove peers that no longer exist
	for id := range peerToggles {
		if _, exists := currentPeers[id]; !exists {
			container.Remove(peerToggles[id].toggle)
			delete(peerToggles, id)
		}
	}
	container.Refresh()

	// Enable or disable buttons based on peer count
	if len(common.AllPeers) > 0 {
		pingButton.Enable()
		heightButton.Enable()
	} else {
		pingButton.Disable()
		heightButton.Disable()
	}
}

// -------- Visuals
// newSpacer creates a separator for UI grouping
func newSpacer() *fyne.Container {
	return container.NewVBox(
		widget.NewLabel(""),
		widget.NewSeparator(),
		widget.NewLabel(""),
	)
}

// createTitleLabel creates a stylized title label
func createTitleLabel(text string) *canvas.Text {
	title := canvas.NewText(text, theme.ForegroundColor())
	title.TextSize = 18
	title.TextStyle = fyne.TextStyle{Bold: true}
	title.Alignment = fyne.TextAlignCenter
	return title
}
