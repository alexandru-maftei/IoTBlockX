package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
)

// IoTDataPacket represents the data packet sent by an IoT device.
type IoTDataPacket struct {
	DeviceID  string `json:"DeviceID"`
	Timestamp string `json:"Timestamp"`
	DataType  string `json:"DataType"`
	Value     int    `json:"Value"`
	SensorID  string `json:"SensorID"`
}

// Sensor represents an IoT sensor with its send time and other attributes.
type Sensor struct {
	ID        string
	DataType  string
	SendTime  time.Time
	StopChan  chan bool
	StartChan chan bool
}

// sendDataHTTP sends data using HTTP with a reusable http.Client.
func sendDataHTTP(client *http.Client, dataPacket IoTDataPacket, gatewayURL string) error {
	jsonData, err := json.Marshal(dataPacket)
	fmt.Println(dataPacket) ///// DELETE THIS
	if err != nil {
		return fmt.Errorf("error marshalling JSON: %v", err)
	}

	// Implement retry logic for sending HTTP requests
	for retries := 0; retries < 3; retries++ {
		resp, err := client.Post(gatewayURL, "application/json", bytes.NewBuffer(jsonData))
		if err == nil {
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusAccepted {
				return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
			}
			return nil
		}
		fmt.Printf("Error sending HTTP request: %v. Retrying... (%d/3)\n", err, retries+1)
		time.Sleep(1 * time.Second) // Backoff before retrying
	}
	return fmt.Errorf("failed to send HTTP request after retries")
}

// sendDataWS sends data using WebSocket.
func sendDataWS(conn *websocket.Conn, dataPacket IoTDataPacket, wsMu *sync.Mutex) error {
	jsonData, err := json.Marshal(dataPacket)
	if err != nil {
		return fmt.Errorf("error marshalling JSON: %v", err)
	}
	wsMu.Lock()
	defer wsMu.Unlock()
	err = conn.WriteMessage(websocket.TextMessage, jsonData)
	if err != nil {
		return fmt.Errorf("error sending WebSocket message: %v", err)
	}
	return nil
}

// IoTDevice simulates an IoT device sending data at regular intervals.
func IoTDevice(sensor Sensor, interval time.Duration, wg *sync.WaitGroup, client *http.Client, gatewayURL string, protocol string, wsConn *websocket.Conn, wsMu *sync.Mutex) {
	defer wg.Done()

	dataRanges := map[string][2]int{
		"temp":   {15, 30},
		"hum":    {30, 70},
		"noise":  {20, 100},
		"CO2":    {300, 1000},
		"press":  {950, 1050},
		"light":  {0, 1000},
		"motion": {0, 1},
		"vib":    {0, 100},
		"wind":   {0, 150},
	}

	// Wait for the start signal
	<-sensor.StartChan

	for {
		select {
		case <-sensor.StopChan:
			fmt.Printf("Device ID: %s has stopped.\n", sensor.ID)
			return
		default:
			if time.Now().After(sensor.SendTime) {
				valueRange := dataRanges[sensor.DataType]
				value := rand.Intn(valueRange[1]-valueRange[0]) + valueRange[0]

				// Create the data packet with the updated structure
				dataPacket := IoTDataPacket{
					DeviceID:  sensor.ID,
					Timestamp: time.Now().UTC().Format(time.RFC3339), // Format timestamp as RFC3339
					DataType:  sensor.DataType,
					Value:     value,
					SensorID:  sensor.ID, // Set to your desired SensorID or pass it dynamically if needed
				}

				// fmt.Printf("Device ID: %s, Data: %s: %d\n", dataPacket.DeviceID, dataPacket.DataType, dataPacket.Value)

				var err error
				if protocol == "http" {
					err = sendDataHTTP(client, dataPacket, gatewayURL)
				} else if protocol == "ws" {
					err = sendDataWS(wsConn, dataPacket, wsMu)
				} else {
					err = fmt.Errorf("unknown protocol: %s", protocol)
				}
				if err != nil {
					fmt.Printf("Error sending data: %v\n", err)
				}

				// Schedule the next send time for the sensor
				sensor.SendTime = sensor.SendTime.Add(interval)
			}
			time.Sleep(100 * time.Millisecond) // Sleep for a short duration to prevent busy-waiting
		}
	}
}

func generateShortUUID() string {
	uuid := uuid.New().String()
	return uuid[:8]
}

func main() {
	var numSensors int
	var totalTime int
	var protocol string
	var wsURL string

	flag.IntVar(&numSensors, "n", 20000, "Total number of IoT sensors to spawn")
	flag.IntVar(&totalTime, "t", 3600, "Total time interval in seconds")
	flag.StringVar(&protocol, "p", "http", "Protocol to use for sending data (http or ws)")
	flag.StringVar(&wsURL, "wsurl", "ws://127.0.0.1:8080/receive-data", "WebSocket URL (used if protocol is ws)")
	flag.Parse()

	if numSensors <= 0 || totalTime <= 0 {
		fmt.Println("Both number of sensors (-n) and total time (-t) must be positive integers.")
		return
	}

	if protocol != "http" && protocol != "ws" {
		fmt.Println("Invalid protocol. Use 'http' or 'ws'.")
		return
	}

	var wsConn *websocket.Conn
	var wsMu sync.Mutex
	if protocol == "ws" {
		var err error
		wsConn, _, err = websocket.DefaultDialer.Dial(wsURL, nil)
		if err != nil {
			fmt.Printf("Error connecting to WebSocket server: %v\n", err)
			return
		}
		defer wsConn.Close()
	}

	rand.Seed(time.Now().UnixNano())

	var wg sync.WaitGroup

	// HTTP client with connection pooling
	client := &http.Client{
		Transport: &http.Transport{
			DialContext: (&net.Dialer{
				Timeout:   30 * time.Second,
				KeepAlive: 30 * time.Second,
			}).DialContext,
			MaxIdleConns:        1000,
			MaxIdleConnsPerHost: 1000,
			IdleConnTimeout:     90 * time.Second,
			TLSHandshakeTimeout: 10 * time.Second,
		},
	}

	// Define the possible data types for the sensors
	dataTypes := []string{"temp", "hum", "noise", "CO2", "press", "light", "motion", "vib", "wind"}

	// Calculate start time
	startTime := time.Now()
	interval := time.Duration(totalTime) * time.Second / time.Duration(numSensors)

	// Initialize sensors
	sensors := make([]Sensor, numSensors)
	for i := 0; i < numSensors; i++ {
		deviceID := generateShortUUID()
		dataType := dataTypes[rand.Intn(len(dataTypes))]
		sendTime := startTime.Add(time.Duration(rand.Intn(totalTime)) * time.Second)
		stopChan := make(chan bool)
		startChan := make(chan bool)

		sensors[i] = Sensor{
			ID:        deviceID,
			DataType:  dataType,
			SendTime:  sendTime,
			StopChan:  stopChan,
			StartChan: startChan,
		}
	}

	// Print the number of sensors initialized
	fmt.Printf("Total number of sensors initialized: %d\n", numSensors)

	// Start IoT devices in parallel
	for i := range sensors {
		wg.Add(1)
		go IoTDevice(sensors[i], interval, &wg, client, "http://127.0.0.1:8080/data", protocol, wsConn, &wsMu)
		close(sensors[i].StartChan)
	}

	// Wait for all goroutines to finish
	wg.Wait()
}
