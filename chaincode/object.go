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
    "github.com/google/uuid"
)

func (s *SmartContract) initobjects(ctx contractapi.TransactionContextInterface) error {
    return nil
}

func (s *SmartContract) GetObjectByPath(ctx contractapi.TransactionContextInterface,
                                        bucket string,
                                        key string) (*Object, error) {
    myuser, err := s.GetMyUser(ctx)
    if err != nil {
        return nil, err
    }

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

    bkt, err := s.GetBucket(ctx, bucket)
    if err != nil {
        return nil, err
    }

    // Test if the ACL says this is ok if this file isn't owned by the user.
    if obj.Owner != myuser.ID {
        ok := false

        // If the object has an ACL, it controls the access. Otherwise, check
        // the bucket's ACL.
        if len(obj.Permissions) != 0 {
            ok = s.testaclaccess(ctx, obj.Permissions, myuser.UID, bucket,
                                 ACL_AccessType_Read)
        } else if len(bkt.Permissions) != 0 {
            ok = s.testaclaccess(ctx, bkt.Permissions, myuser.UID, bucket,
                                 ACL_AccessType_Read)
        }

        if !ok {
            return nil, fmt.Errorf("permission denied")
        }
    }

    return &obj, nil
}

func (s *SmartContract) CreateEmptyObject(ctx contractapi.TransactionContextInterface,
                                          bucket string, key string,
                                          metadata map[string]string,
                                          tags []string,
                                          aclTemplate string,
                                          overwrite bool) (bool, error) {
    nullmd5 := [16]byte { 0xd4, 0x1d, 0x8c, 0xd9, 0x8f, 0x00, 0xb2, 0x04,
                          0xe9, 0x80, 0x09, 0x98, 0xec, 0xf8, 0x42, 0x7e }
    err := s.createobject(ctx, bucket, key, 0, nullmd5, metadata, tags,
                          aclTemplate, ObjectFlag_IndexOnly, overwrite)
    return err == nil, err
}

func (s *SmartContract) CreateObject(ctx contractapi.TransactionContextInterface,
                                     bucket string, key string, size uint64,
                                     md5sum [16]byte,
                                     metadata map[string]string,
                                     tags []string,
                                     aclTemplate string,
                                     overwrite bool) (string, error) {
    err := s.createobject(ctx, bucket, key, size, md5sum, metadata, tags,
                          aclTemplate, 0, overwrite)

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
                                     tags []string,
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

    acl, err := s.getuseraclbyname(ctx, myuser.ID, aclTemplate)
    if err != nil {
        return err
    }

    // Check if the object exists already.
    tmp, _ := s.GetObjectByPath(ctx, bucket, key)
    ok := false
    if tmp != nil {
        if !overwrite {
            return fmt.Errorf("object already exists")
        }

        // If someone else owns the object, check the ACL to see if we can
        // overwrite it or not.
        if tmp.Owner != myuser.ID {
            // If the object has an ACL, it controls the access. Otherwise,
            // check the bucket's ACL.
            if len(tmp.Permissions) != 0 {
                ok = s.testaclaccess(ctx, tmp.Permissions, myuser.UID, bucket,
                                     ACL_AccessType_Overwrite)
            } else if len(bkt.Permissions) != 0 {
                ok = s.testaclaccess(ctx, bkt.Permissions, myuser.UID, bucket,
                                     ACL_AccessType_Overwrite)
            }

            if !ok {
                return fmt.Errorf("permission denied")
            }
        }

        // XXX: Handle removing old object if needed.
    }

    // If we don't already have permission from the above check (for
    // overwriting), check if we have permission to create objects in this
    // bucket.
    if !ok && bkt.Owner != myuser.ID {
        // We only have to check the bucket's acl, because we don't have an
        // object yet in this case.
        if len(bkt.Permissions) != 0 {
            ok = s.testaclaccess(ctx, bkt.Permissions, myuser.UID, bucket,
                                 ACL_AccessType_Create)
        }

        if !ok {
            return fmt.Errorf("permission denied")
        }
    }

    obj := Object {
        Type:           "Object",
        ID:             uuid.NewString(),
        Bucket:         bucket,
        Key:            key,
        Owner:          myuser.ID,
        MD5Sum:         md5sum,
        Size:           size,
        CTime:          time.Now().Unix(),
        Metadata:       metadata,
        Flags:          flags,
        Tags:           tags,
        Permissions:    templatetoacl(acl),
    }

    objJSON, err := json.Marshal(obj)
    if err != nil {
        return err
    }

    sid, _ := ctx.GetStub().CreateCompositeKey("Object", []string{bucket, key})
    err = ctx.GetStub().PutState(sid, objJSON)
    if err != nil {
        return fmt.Errorf("failed to put to world state. %v", err)
    }

    // Add the object to any indexes it belongs in.
    for k, v := range metadata {
        idx, _ := s.getindex(ctx, myuser.ID, k, bucket)
        if idx != nil {
            s.addobjecttoindex(ctx, idx.ID, v, obj.ID)
        }
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

    obj, err := s.GetObjectByPath(ctx, bucket, key)
    if err != nil {
        return "", err
    }

    bkt, err := s.GetBucket(ctx, bucket)
    if err != nil {
        return "", err
    }

    // Test if the ACL says this is ok if this file isn't owned by the user.
    if obj.Owner != myuser.ID {
        ok := false

        // If the object has an ACL, it controls the access. Otherwise, check
        // the bucket's ACL.
        if len(obj.Permissions) != 0 {
            ok = s.testaclaccess(ctx, obj.Permissions, myuser.UID, bucket,
                                 ACL_AccessType_Delete)
        } else if len(bkt.Permissions) != 0 {
            ok = s.testaclaccess(ctx, bkt.Permissions, myuser.UID, bucket,
                                 ACL_AccessType_Delete)
        }

        if !ok {
            return "", fmt.Errorf("permission denied")
        }
    }

    indexFile := (obj.Flags & ObjectFlag_IndexOnly) != 0

    // Create a delete record and save it to world state.
    dr := DeleteRecord {
        Type:           "DeletedObject",
        ID:             obj.ID,
        Bucket:         obj.Bucket,
        Key:            obj.Key,
        Owner:          obj.Owner,
        Deleter:        myuser.ID,
        Permissions:    obj.Permissions,
        MD5Sum:         obj.MD5Sum,
        Size:           obj.Size,
        CTime:          obj.CTime,
        DTime:          time.Now().Unix(),
        Metadata:       obj.Metadata,
        Tags:           obj.Tags,
        Flags:          obj.Flags,
    }

    drJSON, err := json.Marshal(dr)
    if err != nil {
        return "", err
    }

    sidDr, _ := ctx.GetStub().CreateCompositeKey("DeletedObject", []string{obj.ID})
    err = ctx.GetStub().PutState(sidDr, drJSON)
    if err != nil {
        return "", fmt.Errorf("failed to put delete record to world state. %v", err)
    }

    sid, _ := ctx.GetStub().CreateCompositeKey("Object", []string{bucket, key})
    err = ctx.GetStub().DelState(sid)
    if err != nil {
        ctx.GetStub().DelState(sidDr)
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

func (s *SmartContract) isbucketempty(ctx contractapi.TransactionContextInterface,
                                      bucket string) (bool, error) {
    iter, err := ctx.GetStub().GetStateByPartialCompositeKey("Object",
            []string{bucket})
    if err != nil {
        return false, err
    }
    defer iter.Close()

    return !iter.HasNext(), nil
}

func (s *SmartContract) ListObjects(ctx contractapi.TransactionContextInterface,
                                    bucket string, maxobjs uint32,
                                    includeMeta bool,
                                    token string) (*ObjectListing, error) {
    // Set a sane default on the maximum number of objects.
    if maxobjs == 0 || maxobjs > 1000 {
        maxobjs = 1000
    }

    myuser, err := s.GetMyUser(ctx)
    if err != nil {
        return nil, err
    }

    bkt, err := s.GetBucket(ctx, bucket)
    if err != nil {
        return nil, err
    }

    // Test if the ACL says this is ok if this bucket isn't owned by the user.
    if bkt.Owner != myuser.ID {
        ok := false

        if len(bkt.Permissions) != 0 {
            ok = s.testaclaccess(ctx, bkt.Permissions, myuser.UID, bucket,
                                 ACL_AccessType_List)
        }

        if !ok {
            return nil, fmt.Errorf("permission denied")
        }
    }

    iter, meta, err := ctx.GetStub().GetStateByPartialCompositeKeyWithPagination("Object",
            []string{bucket}, int32(maxobjs), token)
    if err != nil {
        return nil, err
    }
    defer iter.Close()

    if meta.FetchedRecordsCount < 0 {
        return nil, fmt.Errorf("Invalid response for object listing")
    }

    objs := make([]ListingObject, meta.FetchedRecordsCount)
    i := 0

    for iter.HasNext() {
        resp, err := iter.Next()
        if err != nil {
            return nil, err
        }

        var obj Object
        err = json.Unmarshal(resp.Value, &obj)
        if err != nil {
            return nil, err
        }

        // Fill in this object.
        objs[i] = ListingObject {
            Key:        obj.Key,
            Owner:      obj.Owner,
            Size:       obj.Size,
            CTime:      obj.CTime,
            MD5Sum:     obj.MD5Sum,
        }

        if includeMeta {
            objs[i].Metadata = obj.Metadata
            objs[i].Tags = obj.Tags
        }

        i++
    }

    // Fill in the metadata wrapping the listing
    rv := ObjectListing {
        Bucket:         bucket,
        Count:          uint64(meta.FetchedRecordsCount),
        Token:          meta.Bookmark,
        Objects:        objs,
    }

    return &rv, nil
}

func (s *SmartContract) QueryObjects(ctx contractapi.TransactionContextInterface,
                                     bucket string, query map[string]string,
                                     maxobjs uint32, includeMeta bool,
                                     token string) (*ObjectListing, error) {
    // Set a sane default on the maximum number of objects.
    if maxobjs == 0 || maxobjs > 1000 {
        maxobjs = 1000
    }

    myuser, err := s.GetMyUser(ctx)
    if err != nil {
        return nil, err
    }

    bkt, err := s.GetBucket(ctx, bucket)
    if err != nil {
        return nil, err
    }

    // Test if the ACL says this is ok if this bucket isn't owned by the user.
    if bkt.Owner != myuser.ID {
        ok := false

        if len(bkt.Permissions) != 0 {
            ok = s.testaclaccess(ctx, bkt.Permissions, myuser.UID, bucket,
                                 ACL_AccessType_List)
        }

        if !ok {
            return nil, fmt.Errorf("permission denied")
        }
    }

    // Build up the metadata portion of the query...
    var querymap map[string]string
    querymap["type"] = "Object"
    querymap["bucket"] = bucket

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
            int32(maxobjs), token)
    if err != nil {
        return nil, err
    }
    defer iter.Close()

    if meta.FetchedRecordsCount < 0 {
        return nil, fmt.Errorf("Invalid response for object listing")
    }

    objs := make([]ListingObject, meta.FetchedRecordsCount)
    i := 0

    for iter.HasNext() {
        resp, err := iter.Next()
        if err != nil {
            return nil, err
        }

        var obj Object
        err = json.Unmarshal(resp.Value, &obj)
        if err != nil {
            return nil, err
        }

        // Fill in this object.
        objs[i] = ListingObject {
            Key:        obj.Key,
            Owner:      obj.Owner,
            Size:       obj.Size,
            CTime:      obj.CTime,
            MD5Sum:     obj.MD5Sum,
        }

        if includeMeta {
            objs[i].Metadata = obj.Metadata
            objs[i].Tags = obj.Tags
        }

        i++
    }

    // Fill in the metadata wrapping the listing
    rv := ObjectListing {
        Bucket:         bucket,
        Count:          uint64(meta.FetchedRecordsCount),
        Token:          meta.Bookmark,
        Objects:        objs,
    }

    return &rv, nil
}

func (s *SmartContract) QueryObjectsByIndex(ctx contractapi.TransactionContextInterface,
                                            bucket string, key string,
                                            value string,
                                            maxobjs uint32, includeMeta bool,
                                            token string) (*ObjectListing, error) {
    // Set a sane default on the maximum number of objects.
    if maxobjs == 0 || maxobjs > 1000 {
        maxobjs = 1000
    }

    myuser, err := s.GetMyUser(ctx)
    if err != nil {
        return nil, err
    }

    bkt, err := s.GetBucket(ctx, bucket)
    if err != nil {
        return nil, err
    }

    // Test if the ACL says this is ok if this bucket isn't owned by the user.
    if bkt.Owner != myuser.ID {
        ok := false

        if len(bkt.Permissions) != 0 {
            ok = s.testaclaccess(ctx, bkt.Permissions, myuser.UID, bucket,
                                 ACL_AccessType_List)
        }

        if !ok {
            return nil, fmt.Errorf("permission denied")
        }
    }

    // Look for an appropriate index
    idx, _ := s.getindex(ctx, myuser.ID, key, bucket)
    if idx == nil {
        return nil, fmt.Errorf("unknown index key")
    }

    // Get the iterator
    iter, err := s.getindexiterator(ctx, idx.ID, value)
    if err != nil {
        return nil, err
    }

    objs := make([]ListingObject, 0)
    i := 0

    for iter.HasNext() {
        resp, err := iter.Next()
        if err != nil {
            return nil, err
        }

        var obj Object
        err = json.Unmarshal(resp.Value, &obj)
        if err != nil {
            return nil, err
        }

        // Fill in this object.
        objs[i] = ListingObject {
            Key:        obj.Key,
            Owner:      obj.Owner,
            Size:       obj.Size,
            CTime:      obj.CTime,
            MD5Sum:     obj.MD5Sum,
        }

        if includeMeta {
            objs[i].Metadata = obj.Metadata
            objs[i].Tags = obj.Tags
        }

        i++
    }

    // Fill in the metadata wrapping the listing
    rv := ObjectListing {
        Bucket:         bucket,
        Count:          uint64(len(objs)),
        Token:          "",
        Objects:        objs,
    }

    return &rv, nil
}

func (s *SmartContract) ListDeletedObjects(ctx contractapi.TransactionContextInterface,
                                           bucket string, maxobjs uint32,
                                           includeMeta bool,
                                           token string) (*ObjectListing, error) {
    // Set a sane default on the maximum number of objects.
    if maxobjs == 0 || maxobjs > 1000 {
        maxobjs = 1000
    }

    myuser, err := s.GetMyUser(ctx)
    if err != nil {
        return nil, err
    }

    bkt, err := s.GetBucket(ctx, bucket)
    if err != nil {
        return nil, err
    }

    // Test if the ACL says this is ok if this bucket isn't owned by the user.
    if bkt.Owner != myuser.ID {
        ok := false

        if len(bkt.Permissions) != 0 {
            ok = s.testaclaccess(ctx, bkt.Permissions, myuser.UID, bucket,
                                 ACL_AccessType_List)
        }

        if !ok {
            return nil, fmt.Errorf("permission denied")
        }
    }

    iter, meta, err := ctx.GetStub().GetStateByPartialCompositeKeyWithPagination("DeletedObject",
            []string{bucket}, int32(maxobjs), token)
    if err != nil {
        return nil, err
    }
    defer iter.Close()

    if meta.FetchedRecordsCount < 0 {
        return nil, fmt.Errorf("Invalid response for object listing")
    }

    objs := make([]ListingObject, meta.FetchedRecordsCount)
    i := 0

    for iter.HasNext() {
        resp, err := iter.Next()
        if err != nil {
            return nil, err
        }

        var obj Object
        err = json.Unmarshal(resp.Value, &obj)
        if err != nil {
            return nil, err
        }

        // Fill in this object.
        objs[i] = ListingObject {
            Key:        obj.Key,
            Owner:      obj.Owner,
            Size:       obj.Size,
            CTime:      obj.CTime,
            MD5Sum:     obj.MD5Sum,
        }

        if includeMeta {
            objs[i].Metadata = obj.Metadata
            objs[i].Tags = obj.Tags
        }

        i++
    }

    // Fill in the metadata wrapping the listing
    rv := ObjectListing {
        Bucket:         bucket,
        Count:          uint64(meta.FetchedRecordsCount),
        Token:          meta.Bookmark,
        Objects:        objs,
    }

    return &rv, nil
}

func (s *SmartContract) QueryDeleteRecords(ctx contractapi.TransactionContextInterface,
                                           bucket string, query map[string]string,
                                           maxobjs uint32, includeMeta bool,
                                           token string) (*ObjectListing, error) {
    // Set a sane default on the maximum number of objects.
    if maxobjs == 0 || maxobjs > 1000 {
        maxobjs = 1000
    }

    myuser, err := s.GetMyUser(ctx)
    if err != nil {
        return nil, err
    }

    bkt, err := s.GetBucket(ctx, bucket)
    if err != nil {
        return nil, err
    }

    // Test if the ACL says this is ok if this bucket isn't owned by the user.
    if bkt.Owner != myuser.ID {
        ok := false

        if len(bkt.Permissions) != 0 {
            ok = s.testaclaccess(ctx, bkt.Permissions, myuser.UID, bucket,
                                 ACL_AccessType_List)
        }

        if !ok {
            return nil, fmt.Errorf("permission denied")
        }
    }

    // Build up the metadata portion of the query...
    var querymap map[string]string
    querymap["type"] = "DeletedObject"
    querymap["bucket"] = bucket

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
            int32(maxobjs), token)
    if err != nil {
        return nil, err
    }
    defer iter.Close()

    if meta.FetchedRecordsCount < 0 {
        return nil, fmt.Errorf("Invalid response for object listing")
    }

    objs := make([]ListingObject, meta.FetchedRecordsCount)
    i := 0

    for iter.HasNext() {
        resp, err := iter.Next()
        if err != nil {
            return nil, err
        }

        var obj Object
        err = json.Unmarshal(resp.Value, &obj)
        if err != nil {
            return nil, err
        }

        // Fill in this object.
        objs[i] = ListingObject {
            Key:        obj.Key,
            Owner:      obj.Owner,
            Size:       obj.Size,
            CTime:      obj.CTime,
            MD5Sum:     obj.MD5Sum,
        }

        if includeMeta {
            objs[i].Metadata = obj.Metadata
            objs[i].Tags = obj.Tags
        }

        i++
    }

    // Fill in the metadata wrapping the listing
    rv := ObjectListing {
        Bucket:         bucket,
        Count:          uint64(meta.FetchedRecordsCount),
        Token:          meta.Bookmark,
        Objects:        objs,
    }

    return &rv, nil
}

func (s *SmartContract) CommitObjectRequest(ctx contractapi.TransactionContextInterface,
                                            bucket string, key string) error {
    // XXX: permission check

    sid, _ := ctx.GetStub().CreateCompositeKey("Object", []string{bucket, key})
    objJSON, err := ctx.GetStub().GetState(sid)
    if err != nil {
        return err
    } else if objJSON == nil {
        return fmt.Errorf("unknown object")
    }

    var obj Object
    err = json.Unmarshal(objJSON, &obj)
    if err != nil {
        return err
    }

    // Remove the staged flag if it is set.
    if (obj.Flags & ObjectFlag_Staged) != 0 {
        obj.Flags &= ^ObjectFlag_Staged
        objJSON, err = json.Marshal(obj)
        if err != nil {
            return err
        }

        err = ctx.GetStub().PutState(sid, objJSON)
    }

    return err
}

