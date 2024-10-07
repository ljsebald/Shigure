/*
    Copyright (C) 2024 Lawrence Sebald
    All Rights Reserved
*/
package main

import (
    "log"

    "github.com/hyperledger/fabric-contract-api-go/v2/contractapi"
    "github.com/ljsebald/shigure-api/chaincode"

    "github.com/minio/minio-go/v7"
    "github.com/minio/minio-go/v7/pkg/credentials"
)

const dishas_endpoint = "127.0.0.1:8080"
const dishas_accesskey = "fill_in_access_key"
const dishas_secretkey = "fill_in_secret_key"

func main() {
    client, err := minio.New(dishas_endpoint, &minio.Options{
        Creds: credentials.NewStaticV4(dishas_accesskey, dishas_secretkey, ""),
        Secure: false,
        BucketLookup: minio.BucketLookupPath,
        Region: "us-east-1",
    })

    if err != nil {
        log.Panicf("Error creating minio client: %v", err)
        return
    }

    shigureChaincode, err := contractapi.NewChaincode(&chaincode.SmartContract{S3client: client})
    if err != nil {
        log.Panicf("Error creating shigure chaincode: %v", err)
    }

    if err := shigureChaincode.Start(); err != nil {
        log.Panicf("Error starting shigure chaincode: %v", err)
    }
}
