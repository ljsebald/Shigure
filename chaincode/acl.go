/*
    Copyright (C) 2024 Lawrence Sebald
    All Rights Reserved
*/
package chaincode

import (
    "encoding/json"
    "fmt"

    "github.com/hyperledger/fabric-contract-api-go/v2/contractapi"
    "github.com/google/uuid"
)

func (s *SmartContract) GetACLByID(ctx contractapi.TransactionContextInterface,
                                   id string) (*ACLTemplate, error) {
    stateid, _ := ctx.GetStub().CreateCompositeKey("ACL", []string{id})
    aclJSON, err := ctx.GetStub().GetState(stateid)

    if err != nil {
        return nil, fmt.Errorf("failed to read from world state: %v", err)
    } else if aclJSON == nil {
        return nil, fmt.Errorf("unknown acl")
    }

    var rv ACLTemplate
    err = json.Unmarshal(aclJSON, &rv)
    if err != nil {
        return nil, err
    }

    return &rv, nil
}

func (s *SmartContract) GetMyACLByName(ctx contractapi.TransactionContextInterface,
                                       name string) (*ACLTemplate, error) {
    myuser, err := s.GetMyUser(ctx)
    if err != nil {
        return nil, err
    }

    return s.getuseraclbyname(ctx, myuser.ID, name)
}

func (s *SmartContract) GetUserACLByName(ctx contractapi.TransactionContextInterface,
                                         uid string,
                                         name string) (*ACLTemplate, error) {
    user, err := s.GetUserByUID(ctx, uid)
    if err != nil {
        return nil, err
    }

    return s.getuseraclbyname(ctx, user.ID, name)
}

func (s *SmartContract) getuseraclbyname(ctx contractapi.TransactionContextInterface,
                                         id string,
                                         name string) (*ACLTemplate, error) {
    // TODO: Use explicit index
    query := fmt.Sprintf(`{"selector":{"type":"ACL","name":"%s","owner":"%s"}}`, name, id)
    resultsIterator, err := ctx.GetStub().GetQueryResult(query)
    if err != nil {
        return nil, err
    }
    defer resultsIterator.Close()

    for resultsIterator.HasNext() {
        queryResponse, err := resultsIterator.Next()
        if err != nil {
            return nil, err
        }

        var acl ACLTemplate
        err = json.Unmarshal(queryResponse.Value, &acl)
        if err != nil {
            return nil, err
        }

        return &acl, nil
    }

    return nil, fmt.Errorf("failed to look up acl for user %s with name: %s", id, name)
}

func (s *SmartContract) CreateACL(ctx contractapi.TransactionContextInterface,
                                  name string, uperms map[string]uint32,
                                  gperms map[string]uint32) (string, error) {
    myuser, err := s.GetMyUser(ctx)
    if err != nil {
        return "", err
    }

    // Make sure we don't already have an ACL template with this name...
    tmp, _ := s.getuseraclbyname(ctx, myuser.ID, name)
    if tmp != nil {
        return "", fmt.Errorf("ACL already exists")
    }

    acl := ACLTemplate {
        Type:           "ACL",
        ID:             uuid.NewString(),
        Owner:          myuser.ID,
        Name:           name,
        Permissions:    make([]ACLEntry, len(uperms) + len(gperms)),
    }

    // Fill in the group and user permissions that were passed in.
    // XXX: Detect duplicates and reject.
    i := 0
    for k, v := range gperms {
        grp, err := s.GetGroupByName(ctx, k)
        if err != nil || grp == nil {
            return "", fmt.Errorf("unknown group %s", k)
        }

        acl.Permissions[i] = ACLEntry {
            ID:             grp.ID,
            Entity:         fmt.Sprintf("Group: %s", k),
            EntryType:      1,
            Permissions:    v,
        }

        i++
    }

    for k, v := range uperms {
        usr, err := s.GetUserByUID(ctx, k)
        if err != nil || usr == nil {
            return "", fmt.Errorf("unknown user %s", k)
        }

        acl.Permissions[i] = ACLEntry {
            ID:             usr.ID,
            Entity:         fmt.Sprintf("User: %s", k),
            EntryType:      0,
            Permissions:    v,
        }

        i++
    }

    aclJSON, err := json.Marshal(acl)
    if err != nil {
        return "", err
    }

    stateid, _ := ctx.GetStub().CreateCompositeKey("ACL", []string{acl.ID})
    err = ctx.GetStub().PutState(stateid, aclJSON)
    if err != nil {
        return "", fmt.Errorf("failed to put to world state. %v", err)
    }

    return acl.ID, nil
}

func (s *SmartContract) DeleteACLEntry(ctx contractapi.TransactionContextInterface,
                                       name string, entrytype uint32,
                                       entity string) (bool, error) {
    acl, err := s.GetMyACLByName(ctx, name)
    if err != nil || acl == nil {
        return false, fmt.Errorf("unknown acl")
    }

    var id string

    if entrytype == 0 {
        usr, err := s.GetUserByUID(ctx, entity)
        if err != nil {
            return false, fmt.Errorf("unknown user")
        }

        id = usr.ID
    } else {
        grp, err := s.GetGroupByName(ctx, entity)
        if err != nil {
            return false, fmt.Errorf("unknown group")
        }

        id = grp.ID
    }

    // Remove any matching elements (there should only be one).
    removed := false
    for i, v := range acl.Permissions {
        if v.EntryType == entrytype && v.ID == id {
            acl.Permissions = append(acl.Permissions[:i],
                                     acl.Permissions[i + 1:]...)
            removed = true
            break
        }
    }

    if !removed {
        return false, nil
    }

    aclJSON, err := json.Marshal(acl)
    if err != nil {
        return false, err
    }

    stateid, _ := ctx.GetStub().CreateCompositeKey("ACL", []string{acl.ID})
    err = ctx.GetStub().PutState(stateid, aclJSON)
    if err != nil {
        return false, fmt.Errorf("failed to put to world state. %v", err)
    }

    return removed, nil
}

func (s *SmartContract) AddACLEntry(ctx contractapi.TransactionContextInterface,
                                    name string, entrytype uint32,
                                    entity string, perms uint32) (bool, error) {
    acl, err := s.GetMyACLByName(ctx, name)
    if err != nil || acl == nil {
        return false, fmt.Errorf("unknown acl")
    }

    var id string

    if entrytype == 0 {
        usr, err := s.GetUserByUID(ctx, entity)
        if err != nil {
            return false, fmt.Errorf("unknown user")
        }

        id = usr.ID
    } else {
        grp, err := s.GetGroupByName(ctx, entity)
        if err != nil {
            return false, fmt.Errorf("unknown group")
        }

        id = grp.ID
    }

    // Find the entry for that entity.
    for _, v := range acl.Permissions {
        if v.EntryType == entrytype && v.ID == id {
            return false, fmt.Errorf("entity already in acl")
        }
    }

    // Add the new entry
    ent := ACLEntry {
        ID:             id,
        Entity:         entity,
        EntryType:      entrytype,
        Permissions:    perms,
    }

    // Update our entry in the db
    acl.Permissions = append(acl.Permissions, ent)
    aclJSON, err := json.Marshal(acl)
    if err != nil {
        return false, err
    }

    stateid, _ := ctx.GetStub().CreateCompositeKey("ACL", []string{acl.ID})
    err = ctx.GetStub().PutState(stateid, aclJSON)
    if err != nil {
        return false, fmt.Errorf("failed to put to world state. %v", err)
    }

    return true, nil
}

func (s *SmartContract) EditACLEntry(ctx contractapi.TransactionContextInterface,
                                     name string, entrytype uint32,
                                     entity string,
                                     perms uint32) (bool, error) {
    acl, err := s.GetMyACLByName(ctx, name)
    if err != nil || acl == nil {
        return false, fmt.Errorf("unknown acl")
    }

    var id string

    if entrytype == 0 {
        usr, err := s.GetUserByUID(ctx, entity)
        if err != nil {
            return false, fmt.Errorf("unknown user")
        }

        id = usr.ID
    } else {
        grp, err := s.GetGroupByName(ctx, entity)
        if err != nil {
            return false, fmt.Errorf("unknown group")
        }

        id = grp.ID
    }

    // Find the entity in question
    found := false
    for i, v := range acl.Permissions {
        if v.EntryType == entrytype && v.ID == id {
            acl.Permissions[i].Permissions = perms
            found = true
            break
        }
    }

    if !found {
        return false, fmt.Errorf("entity not in ACL")
    }

    // Update our entry in the db
    aclJSON, err := json.Marshal(acl)
    if err != nil {
        return false, err
    }

    stateid, _ := ctx.GetStub().CreateCompositeKey("ACL", []string{acl.ID})
    err = ctx.GetStub().PutState(stateid, aclJSON)
    if err != nil {
        return false, fmt.Errorf("failed to put to world state. %v", err)
    }

    return true, nil
}

func (s *SmartContract) DeleteMyACL(ctx contractapi.TransactionContextInterface,
                                    name string) (bool, error) {
    acl, err := s.GetMyACLByName(ctx, name)
    if err != nil || acl == nil {
        return false, fmt.Errorf("unknown acl")
    }

    stateid, _ := ctx.GetStub().CreateCompositeKey("ACL", []string{acl.ID})
    err = ctx.GetStub().DelState(stateid)
    if err != nil {
        return false, err
    }

    return true, nil
}

func (s *SmartContract) DeleteACL(ctx contractapi.TransactionContextInterface,
                                  id string) (bool, error) {
    myuser, err := s.GetMyUser(ctx)
    if err != nil {
        return false, err
    }

    stateid, _ := ctx.GetStub().CreateCompositeKey("ACL", []string{id})
    aclJSON, err := ctx.GetStub().GetState(stateid)

    if err != nil {
        return false, fmt.Errorf("failed to read from world state: %v", err)
    } else if aclJSON == nil {
        return false, fmt.Errorf("unknown acl")
    }

    var acl ACLTemplate
    err = json.Unmarshal(aclJSON, &acl)
    if err != nil {
        return false, err
    }

    // Make sure we have permission
    if acl.Owner != myuser.ID {
        return false, fmt.Errorf("permission denied")
    }

    err = ctx.GetStub().DelState(stateid)
    if err != nil {
        return false, err
    }

    return true, nil
}

func (s *SmartContract) aclExists(ctx contractapi.TransactionContextInterface,
                                   stateid string) (bool, error) {
    aclJSON, err := ctx.GetStub().GetState(stateid)
    if err != nil {
        return false, fmt.Errorf("failed to read from world state: %v", err)
    }

    return aclJSON != nil, nil
}

func (s *SmartContract) ACLExists(ctx contractapi.TransactionContextInterface,
                                  id string) (bool, error) {
    stateid, _ := ctx.GetStub().CreateCompositeKey("ACL", []string{id})
    return s.aclExists(ctx, stateid)
}

func (s *SmartContract) GetAllACLs(ctx contractapi.TransactionContextInterface) ([]*ACLTemplate, error) {
    resultsIterator, err := ctx.GetStub().GetStateByPartialCompositeKey("ACL", []string{})
    if err != nil {
        return nil, err
    }
    defer resultsIterator.Close()

    var acls []*ACLTemplate
    for resultsIterator.HasNext() {
        queryResponse, err := resultsIterator.Next()
        if err != nil {
            return nil, err
        }

        var acl ACLTemplate
        err = json.Unmarshal(queryResponse.Value, &acl)
        if err != nil {
            return nil, err
        }

        acls = append(acls, &acl)
    }

    return acls, nil
}

func (s *SmartContract) GetAllMyACLs(ctx contractapi.TransactionContextInterface) ([]*ACLTemplate, error) {
    myuser, err := s.GetMyUser(ctx)
    if err != nil {
        return nil, err
    }

    query := fmt.Sprintf(`{"selector":{"type":"ACL","owner":"%s"}}`, myuser.ID)
    resultsIterator, err := ctx.GetStub().GetQueryResult(query)
    if err != nil {
        return nil, err
    }
    defer resultsIterator.Close()

    var acls []*ACLTemplate
    for resultsIterator.HasNext() {
        queryResponse, err := resultsIterator.Next()
        if err != nil {
            return nil, err
        }

        var acl ACLTemplate
        err = json.Unmarshal(queryResponse.Value, &acl)
        if err != nil {
            return nil, err
        }

        acls = append(acls, &acl)
    }

    return acls, nil
}

