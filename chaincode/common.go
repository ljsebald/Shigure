/*
    Copyright (C) 2024 Lawrence Sebald
    All Rights Reserved
*/
package chaincode

import (
    "github.com/hyperledger/fabric-contract-api-go/v2/contractapi"
)

type SmartContract struct {
    contractapi.Contract
}

// Sysperms:
// 0x01 = add users
// 0x02 = add subusers of self
// 0x04 = add groups (and subgroups)
// 0x08 = add buckets

// Bucket perms (ACLs are similar):
// 0x01 = list objects (and read metadata)
// 0x02 = read any object
// 0x04 = create objects
// 0x08 = overwrite any object
// 0x10 = delete any object
// 0x20 = reserved
// 0x40 = reserved
// 0x80 = reserved
// low order byte = owner
// 2nd order byte = group
// 3rd order byte = everyone
// high order byte = reserved

type SubUser struct {
    ID              string              `json:"id"`
    UID             string              `json:"uid"`
    Perms           map[string]uint32   `json:"perms"`
}

type User struct {
    Type            string              `json:"type"`
    ID              string              `json:"id"`
    UID             string              `json:"uid"`
    SysPerms        uint32              `json:"sysperms"`
    Parent          string              `json:"parent"`
    SubUsers        []SubUser           `json:"subusers"`
}

type SubGroup struct {
    ID              string              `json:"id"`
    Name            string              `json:"name"`
    Perms           map[string]uint32   `json:"perms"`
}

type Group struct {
    Type            string              `json:"type"`
    ID              string              `json:"id"`
    Name            string              `json:"name"`
    Owner           string              `json:"owner"`
    Parent          string              `json:"parent"`
    Users           []string            `json:"users"`
    SubGroups       []SubGroup          `json:"subgroups"`
}

// EntryType:
// 0 = User
// 1 = Group

type ACLEntry struct {
    ID              string              `json:"id"`
    Entity          string              `json:"entity,omitempty"`
    EntryType       uint32              `json:"enttype"`
    Permissions     uint32              `json:"bits"`
}

type ACL []ACLEntry

type ACLTemplate struct {
    Type            string              `json:"type"`
    ID              string              `json:"id"`
    Owner           string              `json:"owner"`
    Name            string              `json:"name"`
    Permissions     ACL                 `json:"perms"`
}

// AccessType:
// 0 = Read
// 1 = Create
// 2 = Overwrite
// 3 = Delete

type ACLTest struct {
    UID             string              `json:"uid"`
    Bucket          string              `json:"bucket"`
    AccessType      uint32              `json:"access"`
}

type Bucket struct {
    Type            string              `json:"type"`
    Name            string              `json:"name"`
    Owner           string              `json:"owner"`
    Permissions     ACL                 `json:"perms"`
}

// Object Flags:
// 0x01 = Index only file (no data blocks)

type Object struct {
    Type            string              `json:"type"`
    Bucket          string              `json:"bucket"`
    Key             string              `json:"key"`
    Owner           string              `json:"owner"`
    Permissions     ACL                 `json:"perms"`
    MD5Sum          [16]byte            `json:"md5sum"`
    Size            uint64              `json:"size"`
    CTime           int64               `json:"ctime"`
    Metadata        map[string]string   `json:"metadata"`
    Flags           uint64              `json:"flags"`
}

type DeleteRecord struct {
    Type            string              `json:"type"`
    ID              string              `json:"id"`
    Bucket          string              `json:"bucket"`
    Key             string              `json:"key"`
    Owner           string              `json:"owner"`
    Deleter         string              `json:"deleter"`
    Permissions     ACL                 `json:"perms"`
    MD5Sum          [16]byte            `json:"md5sum"`
    Size            uint64              `json:"size"`
    CTime           int64               `json:"ctime"`
    DTime           int64               `json:"dtime"`
    Metadata        map[string]string   `json:"metadata"`
    Flags           uint64              `json:"flags"`
}

func (s *SmartContract) InitLedger(ctx contractapi.TransactionContextInterface) error {
    err := s.initusers(ctx)
    if err != nil {
        return err
    }

    err = s.initgroups(ctx)
    if err != nil {
        return err
    }

    err = s.initacls(ctx)
    if err != nil {
        return err
    }

    err = s.initbuckets(ctx)
    if err != nil {
        return err
    }

    return s.initobjects(ctx)
}

