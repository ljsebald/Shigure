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

// System Permissions
const User_SysPerms_AddUsers    uint32 = 0x01
const User_SysPerms_AddSubUsers uint32 = 0x02
const User_SysPerms_AddGroups   uint32 = 0x04
const User_SysPerms_AddBuckets  uint32 = 0x08

// ACL/Bucket Permissions
const ACL_Perms_ListObjects     uint32 = 0x01
const ACL_Perms_ReadObject      uint32 = 0x02
const ACL_Perms_CreateObject    uint32 = 0x04
const ACL_Perms_OverwriteObject uint32 = 0x08
const ACL_Perms_DeleteObject    uint32 = 0x10
// 0x20+ = Reserved

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

// EntryType
const ACL_EntryType_User    uint32 = 0x00
const ACL_EntryType_Group   uint32 = 0x01

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

const ACL_AccessType_Read       uint32 = 0x00
const ACL_AccessType_Create     uint32 = 0x01
const ACL_AccessType_Overwrite  uint32 = 0x02
const ACL_AccessType_Delete     uint32 = 0x03
const ACL_AccessType_List       uint32 = 0x04

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
    Metadata        map[string]string   `json:"metadata"`
    CTime           int64               `json:"ctime"`
}

// Object Flags:
const ObjectFlag_IndexOnly      uint64 = 0x01
const ObjectFlag_Staged         uint64 = 0x02

type Object struct {
    Type            string              `json:"type"`
    ID              string              `json:"id"`
    Bucket          string              `json:"bucket"`
    Key             string              `json:"key"`
    Owner           string              `json:"owner"`
    Permissions     ACL                 `json:"perms"`
    MD5Sum          [16]byte            `json:"md5sum"`
    Size            uint64              `json:"size"`
    CTime           int64               `json:"ctime"`
    Metadata        map[string]string   `json:"metadata"`
    Tags            []string            `json:"tags"`
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
    Tags            []string            `json:"tags"`
    Flags           uint64              `json:"flags"`
}

type ListingObject struct {
    Key             string              `json:"key"`
    Owner           string              `json:"owner"`
    Size            uint64              `json:"size"`
    CTime           int64               `json:"ctime"`
    MD5Sum          [16]byte            `json:"md5sum"`
    Metadata        map[string]string   `json:"metadata"`
    Tags            []string            `json:"tags"`
}

type ObjectListing struct {
    Bucket          string              `json:"bucket"`
    Count           uint64              `json:"count"`
    Token           string              `json:"token"`
    Objects         []ListingObject     `json:"objects"`
}

type BucketListing struct {
    Count           uint64              `json:"count"`
    Token           string              `json:"token"`
    Buckets         []ListingBucket     `json:"buckets"`
}

type ListingBucket struct {
    Name            string              `json:"name"`
    Owner           string              `json:"owner"`
    CTime           int64               `json:"ctime"`
    Metadata        map[string]string   `json:"metadata"`
}

type UserIndex struct {
    Type            string              `json:"type"`
    ID              string              `json:"id"`
    Owner           string              `json:"owner"`
    Bucket          string              `json:"bucket"`
    Field           string              `json:"field"`
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

    err = s.initobjects(ctx)
    if err != nil {
        return err
    }

    err = s.initindex(ctx)
    if err != nil {
        return err
    }

    return nil
}

