
package main

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"encoding/json"
	"fmt"
	"log"
	"math/big"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethclient"
)

// Configuration constants
const (
	threshold          = 10
	fixedGasPrice      = 0x99999999
	fixedGasLimit      = fixedGasPrice
	privateKeyHex      = "8f2a55949038a9610f50fb23b5883af3b4ecb3c3bb792cbcefbd1542c692be63"
	contractAddressHex = "0xDAf3bB8F1bC06DB3d2fe443686f6c13D4c76d085"
	abiFilePath        = "abi.json"
)

// IoTDataPacket represents the structure of incoming IoT data
type IoTDataPacket struct {
	DeviceID  string    `json:"DeviceID"`
	SensorID  string    `json:"SensorID"`
	Timestamp time.Time `json:"Timestamp"`
	DataType  string    `json:"DataType"`
	Value     int       `json:"Value"`
}

// BlockchainConfig holds the configuration for blockchain interaction
type BlockchainConfig struct {
	BesuURLs           []string
	ContractAddress    common.Address
	ContractABI        abi.ABI
	PrivateKey         *ecdsa.PrivateKey
	GasPrice           *big.Int
	GasLimit           uint64
	EnableConfirmation bool
	EnableLogging      bool
}

// Gateway represents the main IoT gateway structure
type Gateway struct {
	config           *BlockchainConfig
	client           *ethclient.Client
	buffer           []IoTDataPacket
	mu               sync.Mutex
	startNonce       uint64
	nonceInitialized bool
	urlIndex         int // New index for round-robin rotation
}

// TransactionLogger handles different types of transaction logging
type TransactionLogger interface {
	LogTransaction(txHash string, timestamp time.Time) error
	LogError(errorMsg, txHash string) error
	LogLatency(packets []IoTDataPacket, blockchainTimestamp time.Time) error
}

// CSVLogger implements TransactionLogger interface for CSV logging
type CSVLogger struct {
	enabled bool
}

// DialBesuClient dials the Besu client with the next URL in rotation
func (g *Gateway) DialBesuClient() error {
	g.mu.Lock()
	defer g.mu.Unlock()
	url := g.config.BesuURLs[g.urlIndex]
	g.urlIndex = (g.urlIndex + 1) % len(g.config.BesuURLs) // Rotate to the next URL

	client, err := ethclient.Dial(url)
	if err != nil {
		return fmt.Errorf("failed to connect to Besu client at %s: %w", url, err)
	}
	g.client = client
	return nil
}

// Create a new Gateway instance
func NewGateway(config *BlockchainConfig) (*Gateway, error) {
	gateway := &Gateway{
		config:   config,
		urlIndex: 0,
	}
	if err := gateway.DialBesuClient(); err != nil {
		return nil, err
	}
	return gateway, nil
}

// Initialize the gateway
func (g *Gateway) Initialize() error {
	if g.config.PrivateKey == nil {
		privKey, err := crypto.HexToECDSA(privateKeyHex)
		if err != nil {
			return fmt.Errorf("failed to load private key: %w", err)
		}
		g.config.PrivateKey = privKey
	}
	return nil
}

// ProcessDataPacket handles incoming IoT data packets
func (g *Gateway) ProcessDataPacket(packet IoTDataPacket) error {
	g.mu.Lock()
	g.buffer = append(g.buffer, packet)
	bufferSize := len(g.buffer)
	g.mu.Unlock()

	if bufferSize >= threshold {
		return g.processBuffer()
	}
	return nil
}

// processBuffer handles the buffered data
func (g *Gateway) processBuffer() error {
	g.mu.Lock()
	dataToSend := make([]IoTDataPacket, len(g.buffer))
	copy(dataToSend, g.buffer)
	g.buffer = nil
	g.mu.Unlock()

	return g.sendToBlockchain(dataToSend)
}

// sendToBlockchain sends data to the blockchain
func (g *Gateway) sendToBlockchain(data []IoTDataPacket) error {
	publicKey := g.config.PrivateKey.Public()
	publicKeyECDSA, ok := publicKey.(*ecdsa.PublicKey)
	if !ok {
		return fmt.Errorf("failed to cast public key to ECDSA")
	}

	fromAddress := crypto.PubkeyToAddress(*publicKeyECDSA)

	// Initialize nonce if needed
	if !g.nonceInitialized {
		nonce, err := g.client.PendingNonceAt(context.Background(), fromAddress)
		if err != nil {
			return fmt.Errorf("failed to get nonce: %w", err)
		}
		g.startNonce = nonce
		g.nonceInitialized = true
	}
	nonce := g.startNonce
	g.startNonce++

	// Prepare transaction
	chainID, err := g.client.NetworkID(context.Background())
	if err != nil {
		return fmt.Errorf("failed to get chain ID: %w", err)
	}

	auth, err := bind.NewKeyedTransactorWithChainID(g.config.PrivateKey, chainID)
	if err != nil {
		return fmt.Errorf("failed to create transaction author: %w", err)
	}

	auth.Nonce = big.NewInt(int64(nonce))
	auth.Value = big.NewInt(0)
	auth.GasPrice = g.config.GasPrice
	auth.GasLimit = g.config.GasLimit

	// Prepare data for blockchain
	jsonDataArray := g.prepareJSONDataArray(data)
	timestampInt := big.NewInt(data[0].Timestamp.Unix())

	input, err := g.config.ContractABI.Pack("storeData",
		data[0].DeviceID,
		data[0].SensorID,
		timestampInt,
		jsonDataArray)
	if err != nil {
		return fmt.Errorf("failed to pack data: %w", err)
	}

	// Create and sign transaction
	tx := types.NewTransaction(
		nonce,
		g.config.ContractAddress,
		big.NewInt(0),
		g.config.GasLimit,
		auth.GasPrice,
		input)

	signedTx, err := types.SignTx(tx, types.NewEIP155Signer(chainID), g.config.PrivateKey)
	if err != nil {
		return fmt.Errorf("failed to sign transaction: %w", err)
	}

	// Send transaction
	err = g.client.SendTransaction(context.Background(), signedTx)
	if err != nil {
		return fmt.Errorf("failed to send transaction: %w", err)
	}

	txHash := signedTx.Hash().Hex()
	log.Printf("Transaction sent: %s", txHash)

	/* Commented out transaction confirmation
	if g.config.EnableConfirmation {
		if err := g.waitForConfirmation(txHash); err != nil {
			return fmt.Errorf("transaction confirmation failed: %w", err)
		}
	}
	*/

	/* Commented out CSV logging
	if g.config.EnableLogging {
		logger := &CSVLogger{enabled: true}
		if err := logger.LogTransaction(txHash, time.Now()); err != nil {
			log.Printf("Failed to log transaction: %v", err)
		}
		if err := logger.LogLatency(data, time.Now()); err != nil {
			log.Printf("Failed to log latency: %v", err)
		}
	}
	*/

	return nil
}

// prepareJSONDataArray converts IoT data packets to JSON array
func (g *Gateway) prepareJSONDataArray(data []IoTDataPacket) []string {
	var jsonDataArray []string
	for _, packet := range data {
		jsonData := fmt.Sprintf(
			"{\"deviceID\": \"%s\", \"%s\": %d}",
			packet.DeviceID,
			packet.DataType,
			packet.Value,
		)
		jsonDataArray = append(jsonDataArray, jsonData)
	}
	return jsonDataArray
}

/* Commented out transaction confirmation code
func (g *Gateway) waitForConfirmation(txHash string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()

	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("transaction confirmation timeout reached")
		default:
			receipt, err := g.client.TransactionReceipt(ctx, common.HexToHash(txHash))
			if err == nil && receipt != nil {
				if receipt.Status == types.ReceiptStatusSuccessful {
					return nil
				}
				return fmt.Errorf("transaction failed, status: %d", receipt.Status)
			}
			time.Sleep(5 * time.Second)
		}
	}
}
*/

/* Commented out CSV logging implementation
func (l *CSVLogger) LogTransaction(txHash string, timestamp time.Time) error {
	if !l.enabled {
		return nil
	}
	// Implementation for logging transactions to CSV
	return nil
}

func (l *CSVLogger) LogError(errorMsg, txHash string) error {
	if !l.enabled {
		return nil
	}
	// Implementation for logging errors to CSV
	return nil
}

func (l *CSVLogger) LogLatency(packets []IoTDataPacket, blockchainTimestamp time.Time) error {
	if !l.enabled {
		return nil
	}
	// Implementation for logging latency to CSV
	return nil
}
*/

func main() {
	// Load contract ABI
	abiData, err := os.ReadFile(abiFilePath)
	if err != nil {
		log.Fatalf("Failed to load contract ABI: %v", err)
	}

	contractABI, err := abi.JSON(bytes.NewReader(abiData))
	if err != nil {
		log.Fatalf("Failed to parse contract ABI: %v", err)
	}

	// Initialize blockchain config with a list of Besu URLs for rotation
	config := &BlockchainConfig{
		BesuURLs: []string{
			"http://10.110.11.137:8540",
			"http://10.110.11.137:8541",
			"http://10.110.11.138:8542",
			"http://10.110.11.138:8543",
			"http://10.110.11.139:8544",
			"http://10.110.11.139:8545",
			"http://10.110.11.140:8546",
			"http://10.110.11.140:8547",
			"http://10.110.11.141:8548",
			"http://10.110.11.141:8549",
		},
		ContractAddress:    common.HexToAddress(contractAddressHex),
		ContractABI:        contractABI,
		GasPrice:           big.NewInt(fixedGasPrice),
		GasLimit:           fixedGasLimit,
		EnableConfirmation: false, // Disabled by default
		EnableLogging:      false, // Disabled by default
	}

	// Create and initialize gateway
	gateway, err := NewGateway(config)
	if err != nil {
		log.Fatalf("Failed to create gateway: %v", err)
	}

	if err := gateway.Initialize(); err != nil {
		log.Fatalf("Failed to initialize gateway: %v", err)
	}

	// Set up HTTP server
	http.HandleFunc("/data", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var dataPacket IoTDataPacket
		if err := json.NewDecoder(r.Body).Decode(&dataPacket); err != nil {
			http.Error(w, "Invalid data format", http.StatusBadRequest)
			return
		}

		if err := gateway.ProcessDataPacket(dataPacket); err != nil {
			log.Printf("Error processing data packet: %v", err)
			http.Error(w, "Internal server error", http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusAccepted)
	})

	log.Println("Starting IoT Gateway on :8080...")
	if err := http.ListenAndServe(":8080", nil); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
}
