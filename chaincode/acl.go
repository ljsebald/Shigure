/*
    Copyright (C) 2024 Lawrence Sebald
    All Rights Reserved
*/
package chaincode

import (
    "encoding/json"
    "fmt"

    "github.com/hyperledger/fabric-contract-api-go/v2/contractapi"
)

func (s *SmartContract) ReadACL(ctx contractapi.TransactionContextInterface,
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

