// SPDX-License-Identifier: MIT
pragma solidity ^0.8.0;

contract IoTDataStorage {
    struct DataPacket {
        uint256 timestamp;
        string[] jsonData;
    }

    // Maps each sensor ID to an array of its data packets
    mapping(string => DataPacket[]) private sensorData;

    // Event emitted when new data is stored
    event DataStored(string sensorId, uint256 timestamp, string[] jsonData);

    // Stores a new data packet for a specific sensor
    function storeData(string memory sensorId, uint256 timestamp, string[] memory jsonData) public {
        require(bytes(sensorId).length > 0, "Sensor ID cannot be empty");
        require(jsonData.length > 0, "Data cannot be empty");

        sensorData[sensorId].push(DataPacket(timestamp, jsonData));
        emit DataStored(sensorId, timestamp, jsonData);
    }

    // Retrieves all data packets for a given sensor ID
    function getDataBySensorId(string memory sensorId) public view returns (DataPacket[] memory) {
        return sensorData[sensorId];
    }

    // Returns the number of data packets stored for a sensor
    function getPacketCount(string memory sensorId) public view returns (uint256) {
        return sensorData[sensorId].length;
    }
}
