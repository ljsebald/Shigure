/*
    Copyright (C) 2024 Lawrence Sebald
    All Rights Reserved
*/
package main

import (
    "log"

    "github.com/hyperledger/fabric-contract-api-go/v2/contractapi"
    "github.com/ljsebald/shigure-api/chaincode"
)

func main() {
    shigureChaincode, err := contractapi.NewChaincode(&chaincode.SmartContract{})
    if err != nil {
        log.Panicf("Error creating shigure chaincode: %v", err)
    }

    if err := shigureChaincode.Start(); err != nil {
        log.Panicf("Error starting shigure chaincode: %v", err)
    }
}
