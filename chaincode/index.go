/*
    Copyright (C) 2024 Lawrence Sebald
    All Rights Reserved
*/
package chaincode

import (
    "encoding/json"
    "fmt"

    "github.com/hyperledger/fabric-chaincode-go/v2/shim"
    "github.com/hyperledger/fabric-contract-api-go/v2/contractapi"
    "github.com/google/uuid"
)

// Enter the world's crappiest index, version 2.0.
// Now with more abuse of the fact that all of this is essentially backed by
// a key-value store.

// Indexes are stored as Index~Owner~Bucket~MetadataKey
// Entries are "stored" as IndexID~MetadataValue~ObjectID -- the document is an
// empty object.
// We could also store things as IndexID~checksum(MetadataValue)~objectID with
// the document being the full metadata value if we wanted to have less of a
// limitation on the valid set of values... Maybe I'll play around with that
// later, maybe not.
// This does *technically* put a bit more of a limitation on the valid set of
// metadata keys and values, but this isn't a big problem.

func (s *SmartContract) initindex(ctx contractapi.TransactionContextInterface) error {
    return nil
}

func (s *SmartContract) CreateIndex(ctx contractapi.TransactionContextInterface,
                                    field string, bucket string) (bool, error) {
    myuser, err := s.GetMyUser(ctx)
    if err != nil {
        return false, err
    }

    tmp, _ := s.getindex(ctx, myuser.ID, field, bucket)
    if tmp != nil {
        return false, fmt.Errorf("index exists")
    }

    idx := UserIndex {
        Type:       "Index",
        ID:         uuid.NewString(),
        Owner:      myuser.ID,
        Bucket:     bucket,
        Field:      field,
    }

    idxJSON, err := json.Marshal(idx)
    if err != nil {
        return false, err
    }

    sid, _ := ctx.GetStub().CreateCompositeKey("Index", []string{idx.Owner, idx.Bucket, idx.Field})
    err = ctx.GetStub().PutState(sid, idxJSON)
    if err != nil {
        return false, fmt.Errorf("failed to put to world state. %v", err)
    }

    return true, nil
}

func (s *SmartContract) RemoveIndex(ctx contractapi.TransactionContextInterface,
                                    field string, bucket string) (bool, error) {
    myuser, err := s.GetMyUser(ctx)
    if err != nil {
        return false, err
    }

    sid, _ := ctx.GetStub().CreateCompositeKey("Index",
            []string{myuser.ID, bucket, field})
    idxJSON, err := ctx.GetStub().GetState(sid)
    if err != nil {
        return false, err
    } else if idxJSON == nil {
        return false, fmt.Errorf("unknown index")
    }

    var idx UserIndex
    err = json.Unmarshal(idxJSON, &idx)
    if err != nil {
        return false, err
    } 

    err = ctx.GetStub().DelState(sid)
    if err != nil {
        return false, err
    }

    iter, err := ctx.GetStub().GetStateByPartialCompositeKey("IndexEntry",
            []string{idx.ID})
    if err != nil {
        return false, err
    }
    defer iter.Close()

    for iter.HasNext() {
        resp, err := iter.Next()
        if err != nil {
            // XXX: What to do on error here?
            continue
        }

        // XXX: What to do on error here?
        ctx.GetStub().DelState(resp.Key)
    }

    return true, nil
}

func (s *SmartContract) GetIndex(ctx contractapi.TransactionContextInterface,
                                 field string, bucket string) (*UserIndex, error) {
    myuser, err := s.GetMyUser(ctx)
    if err != nil {
        return nil, err
    }

    return s.getindex(ctx, myuser.ID, field, bucket)
}


func (s *SmartContract) getindex(ctx contractapi.TransactionContextInterface,
                                 owner string, field string,
                                 bucket string) (*UserIndex, error) {
    sid, _ := ctx.GetStub().CreateCompositeKey("Index", []string{owner, bucket, field})
    idxJSON, err := ctx.GetStub().GetState(sid)
    if err != nil {
        return nil, err
    } else if idxJSON == nil {
        return nil, fmt.Errorf("unknown index")
    }

    var idx UserIndex
    err = json.Unmarshal(idxJSON, &idx)
    if err != nil {
        return nil, err
    }

    return &idx, nil
}

func (s *SmartContract) addobjecttoindex(ctx contractapi.TransactionContextInterface,
                                         indexid string, value string,
                                         objectid string) error {
    sid, _ := ctx.GetStub().CreateCompositeKey("IndexEntry",
            []string{indexid, value, objectid})
    return ctx.GetStub().PutState(sid, []byte("{}"))
}

func (s *SmartContract) removeobjectfromindex(ctx contractapi.TransactionContextInterface,
                                              indexid string, value string,
                                              objectid string) error {
    sid, _ := ctx.GetStub().CreateCompositeKey("IndexEntry",
            []string{indexid, value, objectid})
    return ctx.GetStub().DelState(sid)
}

func (s *SmartContract) getindexiterator(ctx contractapi.TransactionContextInterface,
                                         indexid string, value string) (shim.StateQueryIteratorInterface, error) {
    return ctx.GetStub().GetStateByPartialCompositeKey("IndexEntry",
            []string{indexid, value})
}

