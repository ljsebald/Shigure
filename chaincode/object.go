/*
    Copyright (C) 2024 Lawrence Sebald
    All Rights Reserved
*/
package chaincode

import (
    "encoding/json"
    "fmt"
    "time"

    "github.com/hyperledger/fabric-contract-api-go/v2/contractapi"
)

func (s *SmartContract) initobjects(ctx contractapi.TransactionContextInterface) error {
    return nil
}

func (s *SmartContract) GetObjectByPath(ctx contractapi.TransactionContextInterface,
                                        bucket string,
                                        key string) (*Object, error) {
    sid, _ := ctx.GetStub().CreateCompositeKey("Object", []string{bucket, key})
    objJSON, err := ctx.GetStub().GetState(sid)
    if err != nil {
        return nil, err
    } else if objJSON == nil {
        return nil, fmt.Errorf("unknown object")
    }

    var obj Object
    err = json.Unmarshal(objJSON, &obj)
    if err != nil {
        return nil, err
    }

    // TODO: Check the ACL.

    return &obj, nil
}

func (s *SmartContract) CreateEmptyObject(ctx contractapi.TransactionContextInterface,
                                          bucket string, key string,
                                          metadata map[string]string,
                                          aclTemplate string,
                                          overwrite bool) (bool, error) {
    nullmd5 := [16]byte { 0xd4, 0x1d, 0x8c, 0xd9, 0x8f, 0x00, 0xb2, 0x04,
                          0xe9, 0x80, 0x09, 0x98, 0xec, 0xf8, 0x42, 0x7e }
    err := s.createobject(ctx, bucket, key, 0, nullmd5, metadata, aclTemplate,
                          overwrite, 0x01)
    return err == nil, err
}

func (s *SmartContract) CreateObject(ctx contractapi.TransactionContextInterface,
                                     bucket string, key string, size uint64,
                                     md5sum [16]byte,
                                     metadata map[string]string,
                                     aclTemplate string,
                                     overwrite bool) (string, error) {
    err := s.createobject(ctx, bucket, key, size, md5sum, metadata, aclTemplate,
                          overwrite, 0)

    if err != nil {
        return "", err
    }

    // TODO: presigned PUT url.
    return "true", err
}

func (s *SmartContract) createobject(ctx contractapi.TransactionContextInterface,
                                     bucket string, key string, size uint64,
                                     md5sum [16]byte,
                                     metadata map[string]string,
                                     aclTemplate string, flags uint64,
                                     overwrite bool) error {
    myuser, err := s.GetMyUser(ctx)
    if err != nil {
        return err
    }

    bkt, err := s.GetBucket(ctx, bucket)
    if err != nil {
        return err
    }

    // Check if the object exists already.
    tmp, _ := s.GetObjectByPath(ctx, bucket, key)
    if tmp != nil {
        if !overwrite {
            return fmt.Errorf("object already exists")
        }

        // TODO: Check the ACL of the object and the bucket to see if we can
        // overwrite the object. Object ACL overrides the bucket one.
        if tmp.Owner != myuser.ID {
            return fmt.Errorf("permission denied")
        }

        // XXX: Handle removing old object if needed.
    }

    // TODO: Do an ACL check if the bucket has one.
    if bkt.Owner != myuser.ID {
        return fmt.Errorf("permission denied")
    }

    obj := Object {
        Type:       "Object",
        Bucket:     bucket,
        Key:        key,
        Owner:      myuser.ID,
        MD5Sum:     md5sum,
        Size:       size,
        CTime:      time.Now().Unix(),
        Metadata:   metadata,
        Flags:      flags,
    }

    // TODO: Add the ACL, if specified.

    objJSON, err := json.Marshal(obj)
    if err != nil {
        return err
    }

    sid, _ := ctx.GetStub().CreateCompositeKey("Object", []string{bucket, key})
    err = ctx.GetStub().PutState(sid, objJSON)
    if err != nil {
        return fmt.Errorf("failed to put to world state. %v", err)
    }

    return nil
}

func (s *SmartContract) RemoveObject(ctx contractapi.TransactionContextInterface,
                                     bucket string,
                                     key string) (string, error) {
    myuser, err := s.GetMyUser(ctx)
    if err != nil {
        return "", err
    }

    obj, err := s.ObjectByPath(ctx, bucket, key)
    if err != nil {
        return "", err
    }

    // TODO: ACL check
    if obj.Owner != myuser.ID {
        return "", fmt.Errorf("permission denied")
    }

    indexFile := (obj.Flags & 0x01) == 0x01

    // TODO: Store Delete Record

    sid, _ := ctx.GetStub().CreateCompositeKey("Object", []string{bucket, key})
    err = ctx.GetStub().DelState(sid)
    if err != nil {
        return "", fmt.Errorf("failed to delete from world state. %v", err)
    }

    // If the Index File flag is set, there was no data for this file on the
    // backing store, so we're done already.
    if indexFile {
        return "true", nil
    }

    // TODO: Generate a presigned url to delete the file.
    return "true", nil
}

