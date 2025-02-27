/*
    Copyright (C) 2024 Lawrence Sebald
    All Rights Reserved
*/
package chaincode

import (
    "encoding/json"
    "fmt"
    "strings"
    "time"

    "github.com/hyperledger/fabric-contract-api-go/v2/contractapi"
)

func (s *SmartContract) initbuckets(ctx contractapi.TransactionContextInterface) error {
    return nil
}

func (s *SmartContract) GetBucket(ctx contractapi.TransactionContextInterface,
                                  name string) (*Bucket, error) {
    sid, _ := ctx.GetStub().CreateCompositeKey("Bucket", []string{name})
    bktJSON, err := ctx.GetStub().GetState(sid)
    if err != nil {
        return nil, err
    } else if bktJSON == nil {
        return nil, fmt.Errorf("unknown bucket")
    }

    var bucket Bucket
    err = json.Unmarshal(bktJSON, &bucket)
    if err != nil {
        return nil, err
    }

    return &bucket, nil
}

func (s *SmartContract) GetMyBuckets(ctx contractapi.TransactionContextInterface) ([]*Bucket, error) {
    myuser, err := s.GetMyUser(ctx)
    if err != nil {
        return nil, err
    }

    return s.getuserbuckets(ctx, myuser.ID)
}

func (s *SmartContract) GetUserBuckets(ctx contractapi.TransactionContextInterface,
                                       uid string) ([]*Bucket, error) {
    user, err := s.GetUserByUID(ctx, uid)
    if err != nil {
        return nil, err
    }

    return s.getuserbuckets(ctx, user.ID)
}

func (s *SmartContract) getuserbuckets(ctx contractapi.TransactionContextInterface,
                                       id string) ([]*Bucket, error) {
    query := fmt.Sprintf(`{"selector":{"type":"Bucket","owner":"%s"}}`, id)
    resultsIterator, err := ctx.GetStub().GetQueryResult(query)
    if err != nil {
        return nil, err
    }
    defer resultsIterator.Close()

    var buckets []*Bucket
    for resultsIterator.HasNext() {
        queryResponse, err := resultsIterator.Next()
        if err != nil {
            return nil, err
        }

        var bucket Bucket
        err = json.Unmarshal(queryResponse.Value, &bucket)
        if err != nil {
            return nil, err
        }

        buckets = append(buckets, &bucket)
    }

    return buckets, nil
}

func (s *SmartContract) AddBucket(ctx contractapi.TransactionContextInterface,
                                  name string,
                                  metadata map[string]string) (string, error) {
    myuser, err := s.GetMyUser(ctx)
    if err != nil {
        return "", err
    }

    if (myuser.SysPerms & User_SysPerms_AddBuckets) == 0 {
        return "", fmt.Errorf("permission denied")
    }

    bkt, _ := s.GetBucket(ctx, name)
    if bkt != nil {
        return "", fmt.Errorf("bucket exists")
    }

    bucket := Bucket {
        Type:           "Bucket",
        Name:           name,
        Owner:          myuser.ID,
        Metadata:       metadata,
        CTime:          time.Now().Unix(),
        Permissions:    make([]ACLEntry, 0),
    }

    bktJSON, err := json.Marshal(bucket)
    if err != nil {
        return "", err
    }

    stateid, _ := ctx.GetStub().CreateCompositeKey("Bucket", []string{name})
    err = ctx.GetStub().PutState(stateid, bktJSON)
    if err != nil {
        return "", fmt.Errorf("failed to put to world state. %v", err)
    }

    return "true", nil
}

func (s *SmartContract) RemoveBucket(ctx contractapi.TransactionContextInterface,
                                     name string) (string, error) {
    myuser, err := s.GetMyUser(ctx)
    if err != nil {
        return "", err
    }

    bkt, err := s.GetBucket(ctx, name)
    if err != nil {
        return "", err
    }

    if bkt.Owner != myuser.ID {
        return "", fmt.Errorf("permission denied")
    }

    empty, err := s.isbucketempty(ctx, name)
    if err != nil {
        return "", err
    } else if !empty {
        return "", fmt.Errorf("bucket not empty")
    }

    stateid, _ := ctx.GetStub().CreateCompositeKey("Bucket", []string{name})
    err = ctx.GetStub().DelState(stateid)
    if err != nil {
        return "", fmt.Errorf("failed to delete from world state. %v", err)
    }

    return "true", nil
}

func (s *SmartContract) SetBucketACLFromTemplate(ctx contractapi.TransactionContextInterface,
                                                 bktname string,
                                                 aclname string) (bool, error) {
    myuser, err := s.GetMyUser(ctx)
    if err != nil {
        return false, err
    }

    bkt, err := s.GetBucket(ctx, bktname)
    if err != nil {
        return false, err
    }

    if bkt.Owner != myuser.ID {
        return false, fmt.Errorf("permission denied")
    }

    tacl, err := s.GetMyACLByName(ctx, aclname)
    if err != nil {
        return false, err
    }

    // Update the state in the db
    bkt.Permissions = templatetoacl(tacl)
    bktJSON, err := json.Marshal(bkt)
    if err != nil {
        return false, err
    }

    stateid, _ := ctx.GetStub().CreateCompositeKey("Bucket", []string{bktname})
    err = ctx.GetStub().PutState(stateid, bktJSON)
    if err != nil {
        return false, fmt.Errorf("failed to put to world state. %v", err)
    }

    return true, nil
}

func (s *SmartContract) QueryMyBuckets(ctx contractapi.TransactionContextInterface,
                                       query map[string]string,
                                       maxbuckets uint32, includeMeta bool,
                                       token string) (*BucketListing, error) {
    // Set a sane default on the maximum number of buckets in one call...
    if maxbuckets == 0 || maxbuckets > 1000 {
        maxbuckets = 1000
    }

    myuser, err := s.GetMyUser(ctx)
    if err != nil {
        return nil, err
    }

    // Build up the metadata portion of the query...
    querymap := make(map[string]string)
    querymap["type"] = "Bucket"
    querymap["owner"] = myuser.ID

    if len(query) > 0 {
        for k, v := range query {
            // Prevent naughty queries....
            if strings.Contains(k, "\"") {
                return nil, fmt.Errorf("invalid query")
            }

            querymap["metadata." + k] = v
        }
    }

    js, err := json.Marshal(querymap)
    if err != nil {
        return nil, err
    }

    dbquery := fmt.Sprintf(`{"selector":%s}`, js)
    iter, meta, err := ctx.GetStub().GetQueryResultWithPagination(dbquery,
            int32(maxbuckets), token)
    if err != nil {
        return nil, err
    }
    defer iter.Close()

    if meta.FetchedRecordsCount < 0 {
        return nil, fmt.Errorf("Invalid response for bucket listing")
    }

    bkts := make([]ListingBucket, meta.FetchedRecordsCount)
    i := 0

    for iter.HasNext() {
        resp, err := iter.Next()
        if err != nil {
            return nil, err
        }

        var bkt Bucket
        err = json.Unmarshal(resp.Value, &bkt)
        if err != nil {
            return nil, err
        }

        // Fill in this bucket.
        bkts[i] = ListingBucket {
            Name:       bkt.Name,
            Owner:      bkt.Owner,
            CTime:      bkt.CTime,
        }

        if includeMeta {
            bkts[i].Metadata = bkt.Metadata
        }

        i++
    }

    // Fill in the metadata wrapping the listing
    rv := BucketListing {
        Count:          uint64(meta.FetchedRecordsCount),
        Token:          meta.Bookmark,
        Buckets:        bkts,
    }

    return &rv, nil
}

